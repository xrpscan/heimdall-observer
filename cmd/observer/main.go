package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/shivanshkc/observer/internal/config"
	"github.com/shivanshkc/observer/internal/logger"
	"github.com/shivanshkc/observer/internal/proc"
	"github.com/shivanshkc/observer/internal/rest"
	"github.com/shivanshkc/observer/internal/store"
	"github.com/shivanshkc/observer/pkg/registry"
	"github.com/shivanshkc/observer/pkg/rippled"
)

// TODO: Add periodic flush to VSC.
// TODO: Make VSC batch size and flush period configurable.

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

	reg.Register("embedded-database", embedded)
	slog.InfoContext(ctx, "successfully connected to the embedded database",
		"path", conf.Database.FilePath)

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

	// Initiate rippled connection.
	ripplec, err := rippled.NewClient(ctx, conf.Ripple.Addr)
	if err != nil {
		slog.ErrorContext(ctx, "error in rippled.NewClient call", "error", err)
		return
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
		slog.ErrorContext(ctx, "failed to subscribe to rippled validation stream", "error", err)
		return
	}

	slog.InfoContext(ctx, "successfully subscribed to the rippled validation stream")

	// Instantiate the validation stream consumer.
	vsc := proc.NewValidationStreamConsumer(validationStreamChan, embedded)
	// VSC is registered after the embedded database so it closes before the database.
	// This is done to make sure that database is running while VSC runs its flush operations.
	reg.Register("validation-stream-consumer", vsc)
	// Start consuming the stream.
	go vsc.Start(ctx)

	// Instantiate the database -> kafka synchronizer.
	dks := proc.NewDatabaseKafkaSynchronizer(embedded, nil /* kafkaProducer */)
	// DKS is intentionally registered after the embedded database and the Kafka producer, since
	// they should close after DKS.
	reg.Register("database-kafka-synchronizer", dks)
	// Start synchronizing.
	go dks.Start(ctx)

	// Block until the app is interrupted or a process calls the CancelFunc.
	<-ctx.Done()
}
