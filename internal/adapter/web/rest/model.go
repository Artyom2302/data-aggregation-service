package rest

import (
	"data-aggregation-service/internal/domain"
	"time"

	"github.com/google/uuid"
)

type GetPackageRequest struct {
	IDs  []uuid.UUID `json:"ids"`
	From time.Time   `json:"from"`
	To   time.Time   `json:"to"`
}

type DataPackageResponse struct {
	ID        uuid.UUID `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	Payload   int       `json:"payload"`
}

func toDataPackagesResponse(packages []domain.DataPackage) []DataPackageResponse {
	resp := make([]DataPackageResponse, 0, len(packages))
	for _, p := range packages {
		resp = append(resp, DataPackageResponse{
			ID:        p.ID,
			CreatedAt: p.CreatedAt,
			Payload:   p.Payload,
		})
	}
	return resp
}
