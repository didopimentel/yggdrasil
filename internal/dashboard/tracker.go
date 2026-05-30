package dashboard

import (
	"sync"

	"github.com/didopimentel/yggdrasil/internal/entities"
)

type serverState struct {
	server  entities.Server
	players map[entities.PlayerID]entities.Position
}

// Tracker maintains an in-memory projection of server and player state for the dashboard API.
type Tracker struct {
	mu        sync.RWMutex
	servers   map[entities.ServerID]*serverState
	playerSrv map[entities.PlayerID]entities.ServerID

	subsMu sync.Mutex
	subs   []chan struct{}
}

func NewTracker() *Tracker {
	return &Tracker{
		servers:   make(map[entities.ServerID]*serverState),
		playerSrv: make(map[entities.PlayerID]entities.ServerID),
	}
}

func (t *Tracker) OnServerRegistered(server entities.Server) {
	t.mu.Lock()
	t.servers[server.ID] = &serverState{
		server:  server,
		players: make(map[entities.PlayerID]entities.Position),
	}
	t.mu.Unlock()
	t.notify()
}

func (t *Tracker) OnServerUnregistered(serverID entities.ServerID) {
	t.mu.Lock()
	if s, ok := t.servers[serverID]; ok {
		for pid := range s.players {
			delete(t.playerSrv, pid)
		}
	}
	delete(t.servers, serverID)
	t.mu.Unlock()
	t.notify()
}

func (t *Tracker) OnPlayerPlaced(playerID entities.PlayerID, serverID entities.ServerID, pos entities.Position) {
	t.mu.Lock()
	t.playerSrv[playerID] = serverID
	if s, ok := t.servers[serverID]; ok {
		s.players[playerID] = pos
	}
	t.mu.Unlock()
	t.notify()
}

// OnPlayerPositionUpdated does not notify subscribers — position churn is handled by the 2s SSE ticker.
func (t *Tracker) OnPlayerPositionUpdated(playerID entities.PlayerID, pos entities.Position) {
	t.mu.Lock()
	if srvID, ok := t.playerSrv[playerID]; ok {
		if s, ok := t.servers[srvID]; ok {
			s.players[playerID] = pos
		}
	}
	t.mu.Unlock()
}

func (t *Tracker) OnPlayerMigrated(playerID entities.PlayerID, toServerID entities.ServerID) {
	t.mu.Lock()
	var pos entities.Position
	if oldID, ok := t.playerSrv[playerID]; ok {
		if old, ok := t.servers[oldID]; ok {
			pos = old.players[playerID]
			delete(old.players, playerID)
		}
	}
	t.playerSrv[playerID] = toServerID
	if newS, ok := t.servers[toServerID]; ok {
		newS.players[playerID] = pos
	}
	t.mu.Unlock()
	t.notify()
}

// ServerSnapshot is the JSON shape sent to the frontend.
type ServerSnapshot struct {
	ID          string `json:"id"`
	Address     string `json:"address"`
	Port        uint32 `json:"port"`
	PlayerCount int    `json:"playerCount"`
}

// PlayerSnapshot is the JSON shape for an individual player.
type PlayerSnapshot struct {
	ID string  `json:"id"`
	X  float64 `json:"x"`
	Y  float64 `json:"y"`
	Z  float64 `json:"z"`
}

func (t *Tracker) Servers() []ServerSnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]ServerSnapshot, 0, len(t.servers))
	for _, s := range t.servers {
		out = append(out, ServerSnapshot{
			ID:          string(s.server.ID),
			Address:     s.server.Address,
			Port:        s.server.Port,
			PlayerCount: len(s.players),
		})
	}
	return out
}

func (t *Tracker) Players(serverID entities.ServerID) []PlayerSnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s, ok := t.servers[serverID]
	if !ok {
		return nil
	}
	out := make([]PlayerSnapshot, 0, len(s.players))
	for id, pos := range s.players {
		out = append(out, PlayerSnapshot{ID: string(id), X: pos.X, Y: pos.Y, Z: pos.Z})
	}
	return out
}

func (t *Tracker) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	t.subsMu.Lock()
	t.subs = append(t.subs, ch)
	t.subsMu.Unlock()
	return ch, func() {
		t.subsMu.Lock()
		for i, s := range t.subs {
			if s == ch {
				t.subs = append(t.subs[:i], t.subs[i+1:]...)
				break
			}
		}
		t.subsMu.Unlock()
	}
}

func (t *Tracker) notify() {
	t.subsMu.Lock()
	for _, ch := range t.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	t.subsMu.Unlock()
}
