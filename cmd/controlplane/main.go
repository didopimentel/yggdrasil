package main

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
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

	// --- Read and validate configuration from environment ---
	cellAmount := uint64(500)
	if v := os.Getenv("YGGDRASIL_CELL_AMOUNT"); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			logger.Error("invalid YGGDRASIL_CELL_AMOUNT", "value", v, "error", err)
			os.Exit(1)
		}
		if parsed == 0 {
			logger.Error("YGGDRASIL_CELL_AMOUNT must be > 0")
			os.Exit(1)
		}
		cellAmount = parsed
	}

	addr := ":9000"
	if v := os.Getenv("YGGDRASIL_ADDR"); v != "" {
		addr = v
	}
	if addr == "" {
		logger.Error("YGGDRASIL_ADDR must be non-empty")
		os.Exit(1)
	}

	serverToCellRatio := 10
	if v := os.Getenv("YGGDRASIL_SERVER_TO_CELL_RATIO"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			logger.Error("invalid YGGDRASIL_SERVER_TO_CELL_RATIO", "value", v, "error", err)
			os.Exit(1)
		}
		if parsed <= 0 {
			logger.Error("YGGDRASIL_SERVER_TO_CELL_RATIO must be > 0")
			os.Exit(1)
		}
		serverToCellRatio = parsed
	}

	// --- Initialize cache ---
	playerPositionCache, err := ristretto.NewCache(&ristretto.Config[entities.PlayerID, entities.Position]{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})
	if err != nil {
		logger.Error("failed to create cache", "error", err)
		os.Exit(1)
	}

	playerServerCache, err := ristretto.NewCache(&ristretto.Config[entities.PlayerID, entities.ServerID]{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})
	if err != nil {
		logger.Error("failed to create cache", "error", err)
		os.Exit(1)
	}

	serverRegistryCache, err := ristretto.NewCache(&ristretto.Config[entities.ServerID, entities.Server]{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})
	if err != nil {
		logger.Error("failed to create cache", "error", err)
		os.Exit(1)
	}

	cellRegistryCache, err := ristretto.NewCache(&ristretto.Config[entities.Cell, entities.ServerID]{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})
	if err != nil {
		logger.Error("failed to create cache", "error", err)
		os.Exit(1)
	}

	serverCellsCache, err := ristretto.NewCache(&ristretto.Config[entities.ServerID, []entities.Cell]{
		NumCounters: 1e7,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})
	if err != nil {
		logger.Error("failed to create cache", "error", err)
		os.Exit(1)
	}

	// --- Initialize repositories ---
	cellRegistryRepository := repository.NewCellRegistryRepository(cellRegistryCache, serverCellsCache, cellAmount)
	playerPositionRepository := repository.NewPlayerPositionRepository(playerPositionCache)
	playerServerRepository := repository.NewPlayerServerRepository(playerServerCache)
	serverRegistryRepository := repository.NewServerRegistryRepository(serverRegistryCache)

	_ = playerPositionRepository
	_ = playerServerRepository

	// --- Init control-plane orchestrator ---
	controlPlane := controlplane.New(logger)
	placement := placement.New(logger)
	serverManager := servermanager.New(logger, serverToCellRatio, cellRegistryRepository, serverRegistryRepository)

	// --- gRPC server ---
	grpcServer := grpc.NewServer()

	pb.RegisterControlServiceServer(grpcServer, controlPlane)
	pb.RegisterPlacementServiceServer(grpcServer, placement)
	pb.RegisterServerManagerServiceServer(grpcServer, serverManager)

	// --- Start listening ---
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("failed to listen", "addr", addr, "error", err)
		os.Exit(1)
	}

	logger.Info("controlplane server starting", "addr", addr)

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
