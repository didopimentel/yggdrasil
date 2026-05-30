package servermanager

import (
	"io"
	"log/slog"

	"github.com/didopimentel/yggdrasil/api/pb"
	"github.com/didopimentel/yggdrasil/internal/entities"
	"github.com/didopimentel/yggdrasil/internal/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ServerObserver is notified of server lifecycle events.
type ServerObserver interface {
	OnServerRegistered(entities.Server)
	OnServerUnregistered(entities.ServerID)
}

type Params struct {
	Logger            *slog.Logger
	ServerToCellRatio int
	CellRegistry      *repository.CellRegistryRepository
	ServerRegistry    *repository.ServerRegistryRepository
	Observer          ServerObserver // optional
}

type ServerManager struct {
	pb.UnimplementedServerManagerServiceServer
	logger            *slog.Logger
	serverToCellRatio int
	cellRegistry      *repository.CellRegistryRepository
	serverRegistry    *repository.ServerRegistryRepository
	observer          ServerObserver
}

func New(p Params) *ServerManager {
	return &ServerManager{
		logger:            p.Logger,
		serverToCellRatio: p.ServerToCellRatio,
		cellRegistry:      p.CellRegistry,
		serverRegistry:    p.ServerRegistry,
		observer:          p.Observer,
	}
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

		serverID := entities.ServerID(server.GetId())
		serverEntity := entities.Server{
			ID:      serverID,
			Address: server.GetAddress(),
			Port:    server.GetPort(),
		}
		sm.cellRegistry.AssignCells(serverID, sm.serverToCellRatio)
		sm.serverRegistry.SetServer(serverID, serverEntity)

		if sm.observer != nil {
			sm.observer.OnServerRegistered(serverEntity)
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

		id := entities.ServerID(serverID.GetId())
		sm.cellRegistry.UnassignServerFromAllCells(id)
		sm.serverRegistry.DeleteServer(id)

		if sm.observer != nil {
			sm.observer.OnServerUnregistered(id)
		}

		if sendErr := stream.Send(&pb.ControlAck{Ok: true}); sendErr != nil {
			return status.Errorf(codes.Internal, "send unregister server ack: %v", sendErr)
		}
	}
}
