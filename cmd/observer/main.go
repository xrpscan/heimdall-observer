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
)

func main() {
	// This is the root context of the app.
	// It is canceled in two cases:
	// 	- If an interruption is detected, or
	//	- If a fatal error occurs that requires the app to exit.
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

	// Set up the API handlers.
	handler := rest.NewHandler(conf)

	// The REST API server of the app.
	httpServer := makeHttpServer(ctx, conf.HttpServer.Addr, handler)

	go func() {
		// Signal the app to exit if the http server stops.
		// This is fine even if the server is stopped by the cleanup function.
		defer cancel()

		slog.InfoContext(ctx, "starting the http server", "addr", conf.HttpServer.Addr)

		// Start listening.
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.ErrorContext(ctx, "error in ListenAndServe call", "error", err)
		}
	}()

	// The app exits only once the root context is canceled.
	<-ctx.Done()
	// Gracefully shutdown services before exiting.
	cleanup(httpServer, handler)
}

// makeHttpServer makes the http server and returns it without calling any Listen methods.
func makeHttpServer(ctx context.Context, addr string, handler http.Handler) *http.Server {
	return &http.Server{
		BaseContext:       func(_ net.Listener) context.Context { return ctx },
		Addr:              addr,
		ReadHeaderTimeout: time.Second * 5,
		ReadTimeout:       0, // Not set to avoid problems with websocket connections.
		WriteTimeout:      0, // Not set to avoid problems with websocket connections.
		IdleTimeout:       time.Second * 60,
		MaxHeaderBytes:    64 * 1024, // 64 KB
		Handler:           handler,
	}
}

// cleanup closes all the passed dependencies gracefully.
// It is supposed to be called before the app exits.
func cleanup(httpServer *http.Server, handler *rest.Handler) {
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

	if handler != nil {
		if err := handler.Close(ctx); err != nil {
			slog.ErrorContext(ctx, "failed to close rest handler", "error", err)
		} else {
			slog.InfoContext(ctx, "rest handler shutdown successful")
		}
	}
}
