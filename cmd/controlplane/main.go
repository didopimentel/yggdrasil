package main

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/controlplane"
	"google.golang.org/grpc"
)

func main() {
	// --- Setup structured logger ---
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// --- Init control-plane orchestrator ---
	controlPlane := controlplane.New(logger)

	// --- gRPC server ---
	grpcServer := grpc.NewServer()

	// Register API implementations (server-side)
	pb.RegisterControlServiceServer(grpcServer, controlPlane)

	// --- Start listening ---
	addr := ":9000" // MVP, can be flag/env configurable
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("failed to listen", "addr", addr, "error", err)
		os.Exit(1)
	}

	logger.Info("controlplane server starting", "addr", addr)

	// Run server in background
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server exited", "error", err)
			os.Exit(1)
		}
	}()

	// --- Graceful shutdown ---
	waitForShutdown(func() {
		logger.Info("shutting down gracefully")
		grpcServer.GracefulStop()
	})
}

// waitForShutdown handles ctrl+c / SIGINT / SIGTERM cleanly.
func waitForShutdown(onShutdown func()) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	s := <-sigCh
	slog.Info("received shutdown signal", "signal", s)
	onShutdown()
}
