package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shivanshkc/observer/internal/config"
	"github.com/shivanshkc/observer/internal/logger"
	"github.com/shivanshkc/observer/internal/proc"
	"github.com/shivanshkc/observer/internal/rest"
	"github.com/shivanshkc/observer/internal/store"
	"github.com/shivanshkc/observer/pkg/registry"
	"github.com/shivanshkc/observer/pkg/rippled"
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
	logger.Init(os.Stdout, conf.Logger.Level, conf.Logger.Pretty)

	// Registry to ensure graceful shutdown.
	reg := registry.New(slog.Default())
	// Close all registered services before application exit.
	defer reg.MustCloseAll()

	// Log config file path along with the working directory to avoid confusions.
	wd, _ := os.Getwd()
	slog.InfoContext(ctx, "config file path", "path", *configPath, "wd", wd)

	// Connect to the embedded database.
	embedded, err := store.NewEmbedded(ctx, conf.Database.FilePath)
	if err != nil {
		slog.ErrorContext(ctx, "failed to connect to embedded database", "error", err)
		return
	}

	reg.Register("embedded-db", embedded)
	slog.InfoContext(ctx, "successfully connected to the embedded database",
		"path", conf.Database.FilePath)

	// Create and register the REST API server of the app.
	httpServer := makeHttpServer(ctx, conf.HttpServer.Addr, rest.NewHandler(conf))
	reg.RegisterWithFunc("http-server", func(ctx context.Context) error {
		return httpServer.Shutdown(ctx)
	})

	go func() {
		// Signal the registry for closure.
		defer cancel()
		slog.InfoContext(ctx, "starting the http-server", "addr", conf.HttpServer.Addr)

		// Start listening.
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
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
	// Since it is being registered after the embedded database initialization, it will be closed
	// before it. Making sure the remaining events are flushed correctly.
	reg.Register("validation-stream-consumer", vsc)
	// Start consuming the stream.
	go vsc.Start(ctx)

	// Block until the app is interrupted or a process calls the CancelFunc.
	<-ctx.Done()
}

// makeHttpServer makes the http server and returns it without calling any Listen methods.
func makeHttpServer(ctx context.Context, addr string, handler http.Handler) *http.Server {
	return &http.Server{
		BaseContext: func(_ net.Listener) context.Context { return ctx },
		Addr:        addr,
		Handler:     handler,
		// Max time to read request headers. Defends against slowloris attacks.
		ReadHeaderTimeout: 5 * time.Second,
		// Max time from connection accept to full request body read.
		ReadTimeout: 5 * time.Second,
		// Max time from request header read to response write completion.
		WriteTimeout: 10 * time.Second,
		// Max time a keep-alive connection can sit idle between requests.
		IdleTimeout: 60 * time.Second,
		// Max size of request headers.
		MaxHeaderBytes: 8 * 1024, // 8 KB
	}
}
