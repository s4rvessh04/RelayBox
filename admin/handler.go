package admin

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"outbox-relay/internal/store"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type AdminHandler struct {
	pgStore *store.PostgresStore
}

func NewAdminHandler(pg *store.PostgresStore) *AdminHandler {
	return &AdminHandler{pgStore: pg}
}

func (h *AdminHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.handleHealth)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("POST /replay", h.handleReplay)
}

func (h *AdminHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := h.pgStore.Ping(ctx); err != nil {
		jsonResponse(w, http.StatusInternalServerError, map[string]string{
			"status": "ERROR",
			"error":  err.Error(),
		})
		return
	}
	jsonResponse(w, http.StatusOK, map[string]string{"status": "OK"})
}

func (h *AdminHandler) handleReplay(w http.ResponseWriter, r *http.Request) {
	eventType := r.URL.Query().Get("event_type")
	ctx := r.Context()

	affected, err := h.pgStore.ResetToPending(ctx, eventType)
	if err != nil {
		jsonResponse(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"message":         fmt.Sprintf("Successfully reset %d events to PENDING", affected),
		"affected_events": affected,
	})
}

func jsonResponse(w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("Error encoding JSON response: %v", err)
	}
}
