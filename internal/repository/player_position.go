package repository

import (
	"sync"

	"github.com/didopimentel/yggdrasil/internal/entities"
)

type PlayerPositionRepository struct {
	mu sync.RWMutex
	m  map[entities.PlayerID]entities.Position
}

func NewPlayerPositionRepository() *PlayerPositionRepository {
	return &PlayerPositionRepository{m: make(map[entities.PlayerID]entities.Position)}
}

func (r *PlayerPositionRepository) GetPlayerPosition(playerID entities.PlayerID) (entities.Position, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.m[playerID]
	return v, ok
}

func (r *PlayerPositionRepository) SetPlayerPosition(playerID entities.PlayerID, position entities.Position) {
	r.mu.Lock()
	r.m[playerID] = position
	r.mu.Unlock()
}

func (r *PlayerPositionRepository) DeletePlayerPosition(playerID entities.PlayerID) {
	r.mu.Lock()
	delete(r.m, playerID)
	r.mu.Unlock()
}
