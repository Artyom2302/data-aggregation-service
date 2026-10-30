package rest

import (
	"bytes"
	"context"
	"data-aggregation-service/internal/domain"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeUseCase struct {
	calls    int
	gotIDs   []uuid.UUID
	gotFrom  time.Time
	gotTo    time.Time
	packages []domain.DataPackage
	err      error
}

func (f *fakeUseCase) GetPackages(_ context.Context, ids []uuid.UUID, from, to time.Time) ([]domain.DataPackage, error) {
	f.calls++
	f.gotIDs, f.gotFrom, f.gotTo = ids, from, to
	return f.packages, f.err
}

const (
	testID    = "5b1f8e0a-1111-4c3b-9f2a-000000000001"
	validBody = `{"ids":["` + testID + `"],"from":"2026-01-01T00:00:00Z","to":"2026-12-01T00:00:00Z"}`
)

func newHandler(uc *fakeUseCase, logOut io.Writer) *Handler {
	log := slog.New(slog.NewTextHandler(logOut, nil))
	return NewHandler(log, uc)
}

func doRequest(t *testing.T, h *Handler, method, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "/packages", strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)
	return rec
}

func TestHandler_GetPackages_Validation(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		contentType string
		body        string
		wantCode    int
	}{
		{"wrong content type", http.MethodPost, "text/plain", validBody, http.StatusUnsupportedMediaType},
		{"no content type", http.MethodPost, "", validBody, http.StatusUnsupportedMediaType},
		{"empty body", http.MethodPost, "application/json", "", http.StatusBadRequest},
		{"broken json", http.MethodPost, "application/json", `{"ids":`, http.StatusBadRequest},
		{"ids wrong type", http.MethodPost, "application/json", `{"ids":"x"}`, http.StatusBadRequest},
		{"invalid uuid", http.MethodPost, "application/json", `{"ids":["not-a-uuid"],"from":"2026-01-01T00:00:00Z","to":"2026-12-01T00:00:00Z"}`, http.StatusBadRequest},
		{"no from", http.MethodPost, "application/json", `{"ids":["` + testID + `"],"to":"2026-12-01T00:00:00Z"}`, http.StatusBadRequest},
		{"no to", http.MethodPost, "application/json", `{"ids":["` + testID + `"],"from":"2026-01-01T00:00:00Z"}`, http.StatusBadRequest},
		{"body too large", http.MethodPost, "application/json", `{"ids":["` + strings.Repeat("a", maxBodySize) + `"]}`, http.StatusRequestEntityTooLarge},
		{"wrong method", http.MethodGet, "application/json", validBody, http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := &fakeUseCase{}
			h := newHandler(uc, io.Discard)

			rec := doRequest(t, h, tt.method, tt.contentType, tt.body)

			if rec.Code != tt.wantCode {
				t.Errorf("status: want %d, got %d (body: %s)", tt.wantCode, rec.Code, rec.Body)
			}
			if uc.calls != 0 {
				t.Errorf("use case must not be called on invalid request, called %d times", uc.calls)
			}
		})
	}
}

func TestHandler_GetPackages(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		packages    []domain.DataPackage
		wantBody    string
	}{
		{
			name:        "found",
			contentType: "application/json",
			packages: []domain.DataPackage{{
				ID:        uuid.MustParse(testID),
				CreatedAt: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
				Payload:   7,
			}},

			wantBody: `[{"id":"` + testID + `","created_at":"2026-05-01T00:00:00Z","payload":7}]`,
		},
		{
			name:        "nothing found returns empty array, not null",
			contentType: "application/json",
			wantBody:    `[]`,
		},
		{
			name:        "content type with charset",
			contentType: "application/json; charset=utf-8",
			wantBody:    `[]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := &fakeUseCase{packages: tt.packages}
			h := newHandler(uc, io.Discard)

			rec := doRequest(t, h, http.MethodPost, tt.contentType, validBody)

			if rec.Code != http.StatusOK {
				t.Fatalf("status: want 200, got %d (body: %s)", rec.Code, rec.Body)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("content type: want application/json, got %q", ct)
			}
			if got := strings.TrimSpace(rec.Body.String()); got != tt.wantBody {
				t.Errorf("body:\nwant %s\ngot  %s", tt.wantBody, got)
			}
		})
	}
}

func TestHandler_GetPackages_RequestParsing(t *testing.T) {
	uc := &fakeUseCase{}
	h := newHandler(uc, io.Discard)

	rec := doRequest(t, h, http.MethodPost, "application/json", validBody)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d (body: %s)", rec.Code, rec.Body)
	}
	if uc.calls != 1 {
		t.Fatalf("use case: want 1 call, got %d", uc.calls)
	}
	if want := []uuid.UUID{uuid.MustParse(testID)}; !slices.Equal(uc.gotIDs, want) {
		t.Errorf("ids: want %v, got %v", want, uc.gotIDs)
	}
	if want := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC); !uc.gotFrom.Equal(want) {
		t.Errorf("from: want %v, got %v", want, uc.gotFrom)
	}
	if want := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC); !uc.gotTo.Equal(want) {
		t.Errorf("to: want %v, got %v", want, uc.gotTo)
	}
}

func TestHandler_GetPackages_UseCaseError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantCode   int
		wantInBody string
		hiddenText string
	}{
		{
			name:       "invalid argument",
			err:        fmt.Errorf("%w: ids are required", domain.ErrInvalidArgument),
			wantCode:   http.StatusBadRequest,
			wantInBody: "ids are required",
		},
		{
			name:       "internal error",
			err:        errors.New("db is down"),
			wantCode:   http.StatusInternalServerError,
			wantInBody: "internal error",
			hiddenText: "db is down",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := &fakeUseCase{err: tt.err}
			var logs bytes.Buffer
			h := newHandler(uc, &logs)

			rec := doRequest(t, h, http.MethodPost, "application/json", validBody)

			if rec.Code != tt.wantCode {
				t.Fatalf("status: want %d, got %d (body: %s)", tt.wantCode, rec.Code, rec.Body)
			}
			if !strings.Contains(rec.Body.String(), tt.wantInBody) {
				t.Errorf("body must contain %q, got %q", tt.wantInBody, rec.Body)
			}
			if tt.hiddenText == "" {
				return
			}
			if strings.Contains(rec.Body.String(), tt.hiddenText) {
				t.Errorf("internal error leaked to client: %s", rec.Body)
			}
			if !strings.Contains(logs.String(), tt.hiddenText) {
				t.Errorf("internal error must be logged, logs: %s", logs.String())
			}
		})
	}
}
