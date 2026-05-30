package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/didopimentel/yggdrasil/internal/entities"
)

// NewHandler returns an HTTP mux exposing the dashboard REST and SSE endpoints.
func NewHandler(t *Tracker) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/state", handleState(t))
	mux.HandleFunc("GET /api/servers/{id}/players", handlePlayers(t))
	mux.HandleFunc("GET /api/events", handleSSE(t))
	return withCORS(mux)
}

func handleState(t *Tracker) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{"servers": t.Servers()})
	}
}

func handlePlayers(t *Tracker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := entities.ServerID(r.PathValue("id"))
		writeJSON(w, map[string]any{"players": t.Players(id)})
	}
}

func handleSSE(t *Tracker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ch, cancel := t.Subscribe()
		defer cancel()

		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		send := func() {
			data, _ := json.Marshal(map[string]any{"servers": t.Servers()})
			fmt.Fprintf(w, "data: %s\n\n", data)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}

		send()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ch:
				send()
			case <-ticker.C:
				send()
			}
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func withCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}
