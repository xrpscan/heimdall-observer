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
	"github.com/shivanshkc/observer/internal/rest"
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

	// Log config file path along with the working directory to avoid confusions.
	wd, _ := os.Getwd()
	slog.InfoContext(ctx, "config file path", "path", *configPath, "wd", wd)

	// The REST API server of the app.
	httpServer := makeHttpServer(ctx, conf.HttpServer.Addr, rest.NewHandler(conf))

	go func() {
		// Signal the app to exit if the http server stops.
		defer cancel()
		slog.InfoContext(ctx, "starting the http server", "addr", conf.HttpServer.Addr)

		// Start listening.
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.ErrorContext(ctx, "error in ListenAndServe call", "error", err)
		}
	}()

	// Initiate rippled connection.
	ripplec, err := rippled.NewClient(ctx, conf.Ripple.Addr)
	if err != nil {
		cleanup(httpServer, nil)
		slog.ErrorContext(ctx, "error in rippled.NewClient call", "error", err)
		return
	}

	// Goroutine to monitor rippled websocket errors.
	go func() {
		// Signal the app to exit if this goroutine returns.
		defer cancel()

		// Listen to rippled errors and trigger shutdown if fatal.
		for err := range ripplec.Errors() {
			// If error is fatal, return from the goroutine, triggering shutdown.
			if errors.Is(err, rippled.ErrFatal) {
				slog.ErrorContext(ctx, "fatal error occurred inside rippled client", "error", err)
				return
			}
			slog.ErrorContext(ctx, "non-fatal error occurred inside rippled client", "error", err)
		}
	}()

	// Subscribe to rippled validation stream.
	validationStreamChan, err := ripplec.SubscribeValidationStream(ctx)
	if err != nil {
		cleanup(httpServer, ripplec)
		panic("failed to subscribe to rippled validation stream: " + err.Error())
	}

	// TODO: Use validation stream.
	_ = validationStreamChan

	// The app exits only once the root context is canceled.
	<-ctx.Done()
	// Gracefully shutdown services before exiting.
	cleanup(httpServer, ripplec)
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

// cleanup closes all the passed dependencies gracefully.
// It is supposed to be called before the app exits.
func cleanup(httpServer *http.Server, ripplec *rippled.Client) {
	// To allow dependencies some time for graceful shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if httpServer != nil {
		if err := httpServer.Shutdown(ctx); err != nil {
			slog.ErrorContext(ctx, "failed to shutdown http server", "error", err)
		} else {
			slog.InfoContext(ctx, "http server shutdown successful")
		}
	}

	if ripplec != nil {
		if err := ripplec.Close("application shutdown"); err != nil {
			slog.ErrorContext(ctx, "failed to shutdown rippled client", "error", err)
		} else {
			slog.InfoContext(ctx, "rippled client shutdown successful")
		}
	}
}
