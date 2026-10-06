package query

import (
	"context"
	"data-aggregation-service/internal/domain"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const MaxIDs = 1000

type Reader interface {
	GetDataPackagesByIdsAndTimeRange(ctx context.Context, ids []uuid.UUID, from, to time.Time) ([]domain.DataPackage, error)
}

type UseCase struct {
	reader Reader
}

func New(reader Reader) *UseCase {
	return &UseCase{
		reader: reader,
	}
}

func (uc *UseCase) GetPackages(ctx context.Context, ids []uuid.UUID, from, to time.Time) ([]domain.DataPackage, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: ids are required", domain.ErrInvalidArgument)
	}
	if len(ids) > MaxIDs {
		return nil, fmt.Errorf("%w: too many ids, max %d", domain.ErrInvalidArgument, MaxIDs)
	}
	if from.After(to) {
		return nil, fmt.Errorf("%w: from must not be after to", domain.ErrInvalidArgument)
	}

	return uc.reader.GetDataPackagesByIdsAndTimeRange(ctx, ids, from, to)
}
