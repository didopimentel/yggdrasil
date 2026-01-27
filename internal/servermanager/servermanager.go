package servermanager

import (
	"io"
	"log/slog"

	"github.com/didopimentel/yggdrasil/api/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ServerManager struct {
	pb.UnimplementedServerManagerServiceServer
	logger *slog.Logger
}

func New(logger *slog.Logger) *ServerManager {
	return &ServerManager{logger: logger}
}

func (sm *ServerManager) RegisterServer(stream grpc.BidiStreamingServer[pb.RegisterServerRequest, pb.ControlAck]) error {
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

		if sendErr := stream.Send(&pb.ControlAck{Ok: true}); sendErr != nil {
			return status.Errorf(codes.Internal, "send register server ack: %v", sendErr)
		}
	}
}

func (sm *ServerManager) UnregisterServer(stream grpc.BidiStreamingServer[pb.UnregisterServerRequest, pb.ControlAck]) error {
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

		if sendErr := stream.Send(&pb.ControlAck{Ok: true}); sendErr != nil {
			return status.Errorf(codes.Internal, "send unregister server ack: %v", sendErr)
		}
	}
}
