package main

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/controlplane"
	"github.com/didopimentel/yggdrasil/internal/entities"
	"github.com/didopimentel/yggdrasil/internal/placement"
	"github.com/didopimentel/yggdrasil/internal/repository"
	"github.com/didopimentel/yggdrasil/internal/servermanager"
	"google.golang.org/grpc"

	"github.com/dgraph-io/ristretto/v2"
)

func main() {
	// --- Setup structured logger ---
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cellAmount := uint64(500)

	// --- Initialize cache ---
	playerPositionCache, err := ristretto.NewCache(&ristretto.Config[entities.PlayerID, entities.Position]{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		logger.Error("failed to create cache", "error", err)
		os.Exit(1)
	}

	playerServerCache, err := ristretto.NewCache(&ristretto.Config[entities.PlayerID, entities.ServerID]{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		logger.Error("failed to create cache", "error", err)
		os.Exit(1)
	}

	serverRegistryCache, err := ristretto.NewCache(&ristretto.Config[entities.ServerID, entities.Server]{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		logger.Error("failed to create cache", "error", err)
		os.Exit(1)
	}

	cellRegistryCache, err := ristretto.NewCache(&ristretto.Config[entities.Cell, entities.ServerID]{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		logger.Error("failed to create cache", "error", err)
		os.Exit(1)
	}

	// --- Initialize repositories ---
	cellRegistryRepository := repository.NewCellRegistryRepository(cellRegistryCache, cellAmount)
	playerPositionRepository := repository.NewPlayerPositionRepository(playerPositionCache)
	playerServerRepository := repository.NewPlayerServerRepository(playerServerCache)
	serverRegistryRepository := repository.NewServerRegistryRepository(serverRegistryCache)

	_ = playerPositionRepository
	_ = playerServerRepository

	// --- Init control-plane orchestrator ---
	controlPlane := controlplane.New(logger)
	placement := placement.New(logger)
	serverManager := servermanager.New(logger, 10, cellRegistryRepository, serverRegistryRepository)

	// --- gRPC server ---
	grpcServer := grpc.NewServer()

	// Register API implementations (server-side)
	pb.RegisterControlServiceServer(grpcServer, controlPlane)
	pb.RegisterPlacementServiceServer(grpcServer, placement)
	pb.RegisterServerManagerServiceServer(grpcServer, serverManager)

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
