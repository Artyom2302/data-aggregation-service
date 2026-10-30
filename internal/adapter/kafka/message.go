package kafka

import (
	"data-aggregation-service/internal/domain"
	"time"

	"github.com/google/uuid"
)

type rawPackageMessage struct {
	ID           uuid.UUID `json:"id"`
	CreatedAt    time.Time `json:"created_at"`
	RandomNumber []int     `json:"random_number"`
}

func (m rawPackageMessage) toDomain() domain.RawPackage {
	return domain.RawPackage{
		ID:            m.ID,
		CreatedAt:     m.CreatedAt,
		RandomNumbers: m.RandomNumber,
	}
}
