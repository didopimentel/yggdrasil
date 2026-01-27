package placement

import (
	"io"
	"log/slog"

	"github.com/didopimentel/yggdrasil/api/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PlacementManager struct {
	pb.UnimplementedPlacementServiceServer
	logger *slog.Logger
}

func New(logger *slog.Logger) *PlacementManager {
	return &PlacementManager{logger: logger}
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

		if sendErr := stream.Send(&pb.ControlAck{Ok: true}); sendErr != nil {
			return status.Errorf(codes.Internal, "send complete migration ack: %v", sendErr)
		}
	}
}
