package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xrpscan/heimdall-observer/internal/config"
	"github.com/xrpscan/heimdall-observer/internal/logger"
	"github.com/xrpscan/heimdall-observer/internal/proc"
	"github.com/xrpscan/heimdall-observer/internal/rest"
	"github.com/xrpscan/heimdall-observer/internal/store"
	"github.com/xrpscan/heimdall-observer/pkg/kafkaesque"
	"github.com/xrpscan/heimdall-observer/pkg/registry"
	"github.com/xrpscan/heimdall-observer/pkg/rippled"
)

func main() {
	// This is the root context of the app.
	// It should be passed to all services of the app (example: http server, database client).
	// It is canceled automatically if an interruption is detected.
	// It should be canceled *manually* by the programmer in case any fatal error occurs.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Allow the user to specify the config path.
	// This makes switching between test and live configs convenient.
	configPath := flag.String("config", "config/config.json", "config file path")
	flag.Parse()

	// Very first dependency of the app.
	conf, err := config.Load(*configPath)
	if err != nil {
		panic("failed to load config: " + err.Error())
	}

	// Setup logger.
	closer := logger.Init(conf.Logger.FilePath, conf.Logger.Level, conf.Logger.Pretty)

	// Registry to ensure graceful shutdown.
	reg := registry.New(slog.Default())
	// Close all registered services before application exit.
	defer reg.MustCloseAll()

	// Register the log file for closing.
	reg.RegisterWithFunc("log-file", func(context.Context) error { return closer() })

	// Log config file path along with the working directory to avoid confusions.
	wd, _ := os.Getwd()
	slog.InfoContext(ctx, "config file path", "path", *configPath, "wd", wd)

	// Connect to the embedded database.
	embedded, err := store.NewEmbedded(ctx, conf.Database.FilePath)
	if err != nil {
		slog.ErrorContext(ctx, "failed to connect to embedded database", "error", err)
		return
	}

	// Register embedded database for cleanup.
	reg.Register("embedded-database", embedded)
	slog.InfoContext(ctx, "successfully connected to the embedded database",
		"path", conf.Database.FilePath)

	// Create Kafka client.
	kafkaClient, err := kafkaesque.NewFranzGoClient(ctx, kafkaesque.ClientParams{
		Brokers:    conf.Kafka.Brokers,
		Username:   conf.Kafka.Username,
		Password:   conf.Kafka.Password,
		CACertPath: conf.Kafka.CACertPath,
	})
	if err != nil {
		slog.ErrorContext(ctx, "failed to create kafka client", "error", err)
		return
	}

	// Register Kafka client for cleanup.
	reg.Register("kafka-client", kafkaClient)
	slog.InfoContext(ctx, "successfully connected to Kafka", "brokers", conf.Kafka.Brokers)

	// Create http server and start listening.
	setupHttpServer(ctx, cancel, conf, reg)

	// Connect with rippled and subscribe to the validations stream.
	validationStreamChan, err := setupRipple(ctx, cancel, conf, reg)
	if err != nil {
		slog.ErrorContext(ctx, "failed to setup ripple", "error", err)
		return
	}

	// Function to produce Kafka messages to a fixed topic.
	kProducerFunc := func(ctx context.Context, payload []byte, headers map[string]string) error {
		return kafkaClient.Produce(ctx, conf.Kafka.ValidationsTopic, payload, headers)
	}

	// The two main long-running processes of the application.
	startVSP(ctx, conf, reg, validationStreamChan, embedded)
	startDKS(ctx, conf, reg, embedded, kProducerFunc)

	// Block until the app is interrupted or a process calls the CancelFunc.
	<-ctx.Done()
}

// setupHttpServer creates a new http server and asynchronously starts listening.
//
// It registers the http server with the registry and also calls cancel if the server errors.
func setupHttpServer(
	ctx context.Context, cancel context.CancelFunc, conf config.Config, reg *registry.Registry,
) {
	// Create and register the REST API server of the app.
	server := rest.NewServer(ctx, conf.HttpServer.Addr, rest.NewHandler(conf))
	reg.Register("http-server", server)

	go func() {
		// Signal the registry for closure.
		defer cancel()
		slog.InfoContext(ctx, "starting the http-server", "addr", conf.HttpServer.Addr)

		// Start listening.
		if err := server.ListenAndServe(); err != nil {
			slog.ErrorContext(ctx, "error in ListenAndServe call", "error", err)
		}
	}()
}

// setupRipple establishes connection with rippled, registers it with the registry for cleanup.
// It listens to rippled websocket errors asynchronously, and calls cancel if a fatal error occurs.
//
// It also subscribes to the validation messages stream and returns a read only channel for the
// caller to access it.
func setupRipple(
	ctx context.Context, cancel context.CancelFunc, conf config.Config, reg *registry.Registry,
) (<-chan rippled.MessageValidationReceived, error) {
	// Initiate rippled connection.
	ripplec, err := rippled.NewClient(ctx, conf.Ripple.Addr)
	if err != nil {
		return nil, fmt.Errorf("error in rippled.NewClient call: %w", err)
	}

	// Register the ripple client for graceful closure.
	reg.Register("ripple-client", ripplec)
	slog.InfoContext(ctx, "successfully connected to rippled", "addr", conf.Ripple.Addr)

	// Goroutine to monitor rippled websocket errors.
	go func() {
		// Signal the registry for closure.
		defer cancel()

		// Listen to rippled errors and trigger shutdown if fatal.
		for err := range ripplec.Errors() {
			// If error is fatal, return from the goroutine, triggering shutdown.
			if errors.Is(err, rippled.ErrFatal) {
				slog.ErrorContext(ctx, "fatal error occurred inside rippled client", "error", err)
				return // trigger the `defer cancel()`
			}
			slog.ErrorContext(ctx, "non-fatal error occurred inside rippled client", "error", err)
		}
	}()

	// Subscribe to rippled validation stream.
	validationStreamChan, err := ripplec.SubscribeValidationStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to rippled validation stream: %w", err)
	}

	slog.InfoContext(ctx, "successfully subscribed to the rippled validation stream")
	return validationStreamChan, nil
}

// startVSP starts and registers the Validation Stream Processor.
func startVSP(
	ctx context.Context, conf config.Config, reg *registry.Registry,
	validationStreamChan <-chan rippled.MessageValidationReceived, embedded store.Client,
) {
	// Parse relevant config.
	mbs := conf.ValidationStreamProcessor.MaxBatchSize
	afd := time.Duration(conf.ValidationStreamProcessor.AutoFlushDelaySec) * time.Second

	// Instantiate the validation stream processor.
	vsp := proc.NewValidationStreamProcessor(validationStreamChan, embedded, mbs, afd)

	// VSP is registered after the embedded database so it closes before the database.
	// This is done to make sure that database is running while VSP runs its flush operations.
	reg.Register("validation-stream-processor", vsp)

	// Start reading the stream.
	go vsp.Start(ctx)

	slog.InfoContext(ctx, "starting the validation stream processor",
		"maxBatchSize", mbs, "autoFlushDelay", afd)
}

// startDKS starts and registers the Database-Kafka Synchronizer.
func startDKS(
	ctx context.Context, conf config.Config, reg *registry.Registry,
	embedded store.Client, kafkaProducer proc.ProducerFunc,
) {
	// Parse relevant config.
	mbs := conf.DatabaseKafkaSynchronizer.MaxBatchSize
	pin := time.Duration(conf.DatabaseKafkaSynchronizer.PollIntervalSec) * time.Second

	// Instantiate the database -> kafka synchronizer.
	dks := proc.NewDatabaseKafkaSynchronizer(embedded, kafkaProducer, mbs, pin)

	// DKS is intentionally registered after the embedded database and the Kafka producer, since
	// they should close after DKS.
	reg.Register("database-kafka-synchronizer", dks)

	// Start synchronizing.
	go dks.Start(ctx)
	slog.InfoContext(ctx, "starting database-kafka synchronization")
}
