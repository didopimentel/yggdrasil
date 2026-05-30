package repository

import (
	"sync"

	"github.com/didopimentel/yggdrasil/internal/entities"
)

type ServerRegistryRepository struct {
	mu sync.RWMutex
	m  map[entities.ServerID]entities.Server
}

func NewServerRegistryRepository() *ServerRegistryRepository {
	return &ServerRegistryRepository{m: make(map[entities.ServerID]entities.Server)}
}

func (r *ServerRegistryRepository) GetServer(serverID entities.ServerID) (entities.Server, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.m[serverID]
	return v, ok
}

func (r *ServerRegistryRepository) SetServer(serverID entities.ServerID, server entities.Server) {
	r.mu.Lock()
	r.m[serverID] = server
	r.mu.Unlock()
}

func (r *ServerRegistryRepository) DeleteServer(serverID entities.ServerID) {
	r.mu.Lock()
	delete(r.m, serverID)
	r.mu.Unlock()
}
