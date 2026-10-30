package domain

import (
	"slices"
	"time"

	"github.com/google/uuid"
)

// RawPackage — входные данные до агрегации.
type RawPackage struct {
	ID            uuid.UUID
	CreatedAt     time.Time
	RandomNumbers []int
}

type DataPackage struct {
	ID        uuid.UUID
	CreatedAt time.Time
	Payload   int
}

func NewDataPackage(raw RawPackage) (DataPackage, error) {
	if len(raw.RandomNumbers) == 0 {
		return DataPackage{}, ErrEmptyPackage
	}

	return DataPackage{
		ID:        raw.ID,
		CreatedAt: raw.CreatedAt,
		Payload:   slices.Max(raw.RandomNumbers),
	}, nil
}
