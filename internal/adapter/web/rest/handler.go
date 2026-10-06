package rest

import (
	"bytes"
	"context"
	"data-aggregation-service/internal/domain"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"time"

	"github.com/google/uuid"
)

const (
	maxBodySize = 1 << 20
)

type PackageGetter interface {
	GetPackages(ctx context.Context, ids []uuid.UUID, from, to time.Time) ([]domain.DataPackage, error)
}

type Handler struct {
	log     *slog.Logger
	useCase PackageGetter
}

func NewHandler(log *slog.Logger, useCase PackageGetter) *Handler {
	return &Handler{
		log:     log,
		useCase: useCase,
	}
}

func (h *Handler) GetPackages(w http.ResponseWriter, r *http.Request) error {
	var request GetPackageRequest
	if err := decodeJSON(w, r, &request); err != nil {
		return err
	}
	if request.From.IsZero() || request.To.IsZero() {
		return fmt.Errorf("%w: from and to are required", ErrBadRequest)
	}
	packages, err := h.useCase.GetPackages(r.Context(), request.IDs, request.From, request.To)
	if err != nil {
		return err
	}
	return encodeJSON(w, toDataPackagesResponse(packages))
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != "application/json" {
		return ErrUnsupportedMediaType
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return err
		}
		return fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	return nil
}

func encodeJSON(w http.ResponseWriter, src any) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err := w.Write(buf.Bytes())
	return err
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /packages", h.wrap(h.GetPackages))
	return mux
}

func (h *Handler) wrap(fn func(w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := fn(w, r)
		if err == nil {
			return
		}
		var maxErr *http.MaxBytesError
		switch {
		case errors.As(err, &maxErr):
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		case errors.Is(err, ErrUnsupportedMediaType):
			http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
		case errors.Is(err, domain.ErrInvalidArgument), errors.Is(err, ErrBadRequest):
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			h.log.Error("error in handler", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
	}
}
