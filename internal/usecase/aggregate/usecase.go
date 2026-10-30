package aggregate

import (
	"context"
	"data-aggregation-service/internal/domain"
	"fmt"
)

type Saver interface {
	SaveDataPackage(ctx context.Context, dataPackage domain.DataPackage) error
}

type UseCase struct {
	saver Saver
}

func New(saver Saver) *UseCase {
	return &UseCase{
		saver: saver,
	}
}

func (uc *UseCase) Aggregate(ctx context.Context, raw domain.RawPackage) error {
	pkg, err := domain.NewDataPackage(raw)
	if err != nil {
		return fmt.Errorf("aggregate package %s: %w", raw.ID, err)
	}

	return uc.saver.SaveDataPackage(ctx, pkg)
}
