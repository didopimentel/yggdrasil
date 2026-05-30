package repository

import (
	"sync"

	"github.com/didopimentel/yggdrasil/internal/entities"
)

type PlayerServerRepository struct {
	mu sync.RWMutex
	m  map[entities.PlayerID]entities.ServerID
}

func NewPlayerServerRepository() *PlayerServerRepository {
	return &PlayerServerRepository{m: make(map[entities.PlayerID]entities.ServerID)}
}

func (r *PlayerServerRepository) GetPlayerServer(playerID entities.PlayerID) (entities.ServerID, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.m[playerID]
	return v, ok
}

func (r *PlayerServerRepository) SetPlayerServer(playerID entities.PlayerID, serverID entities.ServerID) {
	r.mu.Lock()
	r.m[playerID] = serverID
	r.mu.Unlock()
}

func (r *PlayerServerRepository) DeletePlayerServer(playerID entities.PlayerID) {
	r.mu.Lock()
	delete(r.m, playerID)
	r.mu.Unlock()
}
