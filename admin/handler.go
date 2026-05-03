package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// AdminStore defines the minimal store interface required by the admin handler.
type AdminStore interface {
	Ping(ctx context.Context) error
	ResetToPending(ctx context.Context, eventType string) (int64, error)
}

type AdminHandler struct {
	pgStore    AdminStore
	adminToken string
}

func NewAdminHandler(pg AdminStore, adminToken string) *AdminHandler {
	return &AdminHandler{pgStore: pg, adminToken: adminToken}
}

func (h *AdminHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.handleHealth)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("POST /replay", h.authMiddleware(h.handleReplay))
}

// authMiddleware checks for a valid Bearer token if ADMIN_TOKEN is configured.
// If no token is configured, auth is skipped (for local development).
func (h *AdminHandler) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.adminToken != "" {
			auth := r.Header.Get("Authorization")
			if !strings.HasPrefix(auth, "Bearer ") || strings.TrimPrefix(auth, "Bearer ") != h.adminToken {
				jsonResponse(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
		}
		next(w, r)
	}
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
