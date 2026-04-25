package placement

import (
	"context"
	"io"
	"log/slog"

	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/entities"
	"github.com/didopimentel/yggdrasil/internal/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MigrationNotifier sends a migration event to the server that currently owns the player.
type MigrationNotifier interface {
	NotifyMigration(ctx context.Context, playerID entities.PlayerID, oldServerID entities.ServerID, newServer entities.Server) error
}

type Params struct {
	Logger             *slog.Logger
	CellRegistry       *repository.CellRegistryRepository
	PlayerPositionRepo *repository.PlayerPositionRepository
	PlayerServerRepo   *repository.PlayerServerRepository
	ServerRegistry     *repository.ServerRegistryRepository
	Grid               entities.Grid
	MigrationNotifier  MigrationNotifier
}

type PlacementManager struct {
	pb.UnimplementedPlacementServiceServer
	logger             *slog.Logger
	cellRegistry       *repository.CellRegistryRepository
	playerPositionRepo *repository.PlayerPositionRepository
	playerServerRepo   *repository.PlayerServerRepository
	serverRegistry     *repository.ServerRegistryRepository
	grid               entities.Grid
	migrationNotifier  MigrationNotifier
}

func New(p Params) *PlacementManager {
	return &PlacementManager{
		logger:             p.Logger,
		cellRegistry:       p.CellRegistry,
		playerPositionRepo: p.PlayerPositionRepo,
		playerServerRepo:   p.PlayerServerRepo,
		serverRegistry:     p.ServerRegistry,
		grid:               p.Grid,
		migrationNotifier:  p.MigrationNotifier,
	}
}

func (pm *PlacementManager) AssignPlayer(stream grpc.BidiStreamingServer[pb.AssignPlayerRequest, pb.ControlAck]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "receive assign player: %v", err)
		}

		player := req.GetPlayerId()
		pos := req.GetPosition()
		if player == nil || player.GetId() == "" {
			if sendErr := stream.Send(&pb.ControlAck{Ok: false, Message: "player_id is required"}); sendErr != nil {
				return status.Errorf(codes.Internal, "send assign player ack: %v", sendErr)
			}
			continue
		}
		if pos == nil {
			if sendErr := stream.Send(&pb.ControlAck{Ok: false, Message: "position is required"}); sendErr != nil {
				return status.Errorf(codes.Internal, "send assign player ack: %v", sendErr)
			}
			continue
		}

		cell := pm.grid.CellAt(entities.Position{X: pos.GetX(), Y: pos.GetY(), Z: pos.GetZ()})
		serverID, ok := pm.cellRegistry.GetCellOwner(cell)
		if !ok {
			if sendErr := stream.Send(&pb.ControlAck{Ok: false, Message: "no server available for cell"}); sendErr != nil {
				return status.Errorf(codes.Internal, "send assign player ack: %v", sendErr)
			}
			continue
		}

		pm.playerPositionRepo.SetPlayerPosition(entities.PlayerID(player.GetId()), entities.Position{X: pos.GetX(), Y: pos.GetY(), Z: pos.GetZ()})
		pm.playerServerRepo.SetPlayerServer(entities.PlayerID(player.GetId()), serverID)

		if sendErr := stream.Send(&pb.ControlAck{Ok: true}); sendErr != nil {
			return status.Errorf(codes.Internal, "send assign player ack: %v", sendErr)
		}
	}
}

func (pm *PlacementManager) UpdatePlayerPosition(stream grpc.BidiStreamingServer[pb.UpdatePlayerPositionRequest, pb.ControlAck]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "receive update player position: %v", err)
		}

		player := req.GetPlayerId()
		pos := req.GetPosition()
		if player == nil || player.GetId() == "" {
			if sendErr := stream.Send(&pb.ControlAck{Ok: false, Message: "player_id is required"}); sendErr != nil {
				return status.Errorf(codes.Internal, "send update player position ack: %v", sendErr)
			}
			continue
		}
		if pos == nil {
			if sendErr := stream.Send(&pb.ControlAck{Ok: false, Message: "position is required"}); sendErr != nil {
				return status.Errorf(codes.Internal, "send update player position ack: %v", sendErr)
			}
			continue
		}

		playerID := entities.PlayerID(player.GetId())
		newPos := entities.Position{X: pos.GetX(), Y: pos.GetY(), Z: pos.GetZ()}

		oldPos, found := pm.playerPositionRepo.GetPlayerPosition(playerID)
		pm.playerPositionRepo.SetPlayerPosition(playerID, newPos)

		if found {
			oldCell := pm.grid.CellAt(oldPos)
			newCell := pm.grid.CellAt(newPos)
			if oldCell != newCell {
				newServerID, ok := pm.cellRegistry.GetCellOwner(newCell)
				oldServerID, _ := pm.playerServerRepo.GetPlayerServer(playerID)
				if ok && newServerID != oldServerID {
					newServerEntity, ok := pm.serverRegistry.GetServer(newServerID)
					if ok {
						_ = pm.migrationNotifier.NotifyMigration(stream.Context(), playerID, oldServerID, newServerEntity)
					}
				}
			}
		}

		if sendErr := stream.Send(&pb.ControlAck{Ok: true}); sendErr != nil {
			return status.Errorf(codes.Internal, "send update player position ack: %v", sendErr)
		}
	}
}

func (pm *PlacementManager) CompleteMigration(stream grpc.BidiStreamingServer[pb.CompleteMigrationRequest, pb.ControlAck]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "receive complete migration: %v", err)
		}

		player := req.GetPlayerId()
		serverID := req.GetServerId()
		if player == nil || player.GetId() == "" {
			if sendErr := stream.Send(&pb.ControlAck{Ok: false, Message: "player_id is required"}); sendErr != nil {
				return status.Errorf(codes.Internal, "send complete migration ack: %v", sendErr)
			}
			continue
		}
		if serverID == nil || serverID.GetId() == "" {
			if sendErr := stream.Send(&pb.ControlAck{Ok: false, Message: "server_id is required"}); sendErr != nil {
				return status.Errorf(codes.Internal, "send complete migration ack: %v", sendErr)
			}
			continue
		}

		pm.playerServerRepo.SetPlayerServer(entities.PlayerID(player.GetId()), entities.ServerID(serverID.GetId()))

		if sendErr := stream.Send(&pb.ControlAck{Ok: true}); sendErr != nil {
			return status.Errorf(codes.Internal, "send complete migration ack: %v", sendErr)
		}
	}
}
