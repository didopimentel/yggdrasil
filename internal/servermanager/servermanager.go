package servermanager

import (
	"log/slog"

	"github.com/didopimentel/yggdrasil/api/pb"
)

type ServerManager struct {
	logger *slog.Logger
}

func New(logger *slog.Logger) *ServerManager {
	return &ServerManager{logger: logger}
}

func (m *ServerManager) RegisterServer(server *pb.Server) error {
	m.logger.Info("register server", "server_id", server.GetId(), "address", server.GetAddress())
	return nil
}

func (m *ServerManager) UnregisterServer(serverID *pb.ServerId) error {
	m.logger.Info("unregister server", "server_id", serverID.GetId())
	return nil
}
