package controlplane

import (
	"io"
	"log/slog"

	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/placement"
	"github.com/didopimentel/yggdrasil/internal/servermanager"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ControlPlane struct {
	pb.UnimplementedControlServiceServer
	logger           *slog.Logger
	serverManager    *servermanager.ServerManager
	placementManager *placement.PlacementManager
}

func New(logger *slog.Logger) *ControlPlane {
	return &ControlPlane{
		logger:           logger,
		serverManager:    servermanager.New(logger),
		placementManager: placement.New(logger),
	}
}

func (cp *ControlPlane) OpenControlStream(req *pb.OpenControlStreamRequest, stream grpc.ServerStreamingServer[pb.ControlEvent]) error {
	if req.ServerId == nil {
		cp.logger.Warn("control stream opened with no server ID")
		return status.Error(codes.InvalidArgument, "server ID is required")
	}

	cp.logger.Info("control stream opened", "server_id", req.ServerId.Id)

	return nil
}

func (cp *ControlPlane) RegisterServer(stream grpc.BidiStreamingServer[pb.RegisterServerRequest, pb.ControlAck]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "receive register server: %v", err)
		}

		server := req.GetServer()
		if server == nil || server.GetId() == "" {
			if sendErr := stream.Send(&pb.ControlAck{Ok: false, Message: "server.id is required"}); sendErr != nil {
				return status.Errorf(codes.Internal, "send register server ack: %v", sendErr)
			}
			continue
		}

		if err := cp.serverManager.RegisterServer(server); err != nil {
			if sendErr := stream.Send(&pb.ControlAck{Ok: false, Message: err.Error()}); sendErr != nil {
				return status.Errorf(codes.Internal, "send register server ack: %v", sendErr)
			}
			continue
		}

		if sendErr := stream.Send(&pb.ControlAck{Ok: true}); sendErr != nil {
			return status.Errorf(codes.Internal, "send register server ack: %v", sendErr)
		}
	}
}

func (cp *ControlPlane) UnregisterServer(stream grpc.BidiStreamingServer[pb.UnregisterServerRequest, pb.ControlAck]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "receive unregister server: %v", err)
		}

		serverID := req.GetServerId()
		if serverID == nil || serverID.GetId() == "" {
			if sendErr := stream.Send(&pb.ControlAck{Ok: false, Message: "server_id is required"}); sendErr != nil {
				return status.Errorf(codes.Internal, "send unregister server ack: %v", sendErr)
			}
			continue
		}

		if err := cp.serverManager.UnregisterServer(serverID); err != nil {
			if sendErr := stream.Send(&pb.ControlAck{Ok: false, Message: err.Error()}); sendErr != nil {
				return status.Errorf(codes.Internal, "send unregister server ack: %v", sendErr)
			}
			continue
		}

		if sendErr := stream.Send(&pb.ControlAck{Ok: true}); sendErr != nil {
			return status.Errorf(codes.Internal, "send unregister server ack: %v", sendErr)
		}
	}
}

func (cp *ControlPlane) AssignPlayer(stream grpc.BidiStreamingServer[pb.AssignPlayerRequest, pb.ControlAck]) error {
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

		if err := cp.placementManager.AssignPlayer(player, pos); err != nil {
			if sendErr := stream.Send(&pb.ControlAck{Ok: false, Message: err.Error()}); sendErr != nil {
				return status.Errorf(codes.Internal, "send assign player ack: %v", sendErr)
			}
			continue
		}

		if sendErr := stream.Send(&pb.ControlAck{Ok: true}); sendErr != nil {
			return status.Errorf(codes.Internal, "send assign player ack: %v", sendErr)
		}
	}
}

func (cp *ControlPlane) UpdatePlayerPosition(stream grpc.BidiStreamingServer[pb.UpdatePlayerPositionRequest, pb.ControlAck]) error {
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

		if err := cp.placementManager.UpdatePlayerPosition(player, pos); err != nil {
			if sendErr := stream.Send(&pb.ControlAck{Ok: false, Message: err.Error()}); sendErr != nil {
				return status.Errorf(codes.Internal, "send update player position ack: %v", sendErr)
			}
			continue
		}

		if sendErr := stream.Send(&pb.ControlAck{Ok: true}); sendErr != nil {
			return status.Errorf(codes.Internal, "send update player position ack: %v", sendErr)
		}
	}
}

func (cp *ControlPlane) CompleteMigration(stream grpc.BidiStreamingServer[pb.CompleteMigrationRequest, pb.ControlAck]) error {
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

		if err := cp.placementManager.CompleteMigration(player, serverID); err != nil {
			if sendErr := stream.Send(&pb.ControlAck{Ok: false, Message: err.Error()}); sendErr != nil {
				return status.Errorf(codes.Internal, "send complete migration ack: %v", sendErr)
			}
			continue
		}

		if sendErr := stream.Send(&pb.ControlAck{Ok: true}); sendErr != nil {
			return status.Errorf(codes.Internal, "send complete migration ack: %v", sendErr)
		}
	}
}
