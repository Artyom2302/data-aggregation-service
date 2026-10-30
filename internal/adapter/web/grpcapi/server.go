package grpcapi

import (
	"context"
	pb "data-aggregation-service/gen/dataaggregation/v1"
	"data-aggregation-service/internal/domain"
	"data-aggregation-service/internal/usecase/query"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type PackageGetter interface {
	GetPackages(ctx context.Context, ids []uuid.UUID, from, to time.Time) ([]domain.DataPackage, error)
}

type Server struct {
	pb.UnimplementedDataAggregationServiceServer
	log     *slog.Logger
	useCase PackageGetter
}

func NewServer(log *slog.Logger, useCase PackageGetter) *Server {
	return &Server{
		log:     log,
		useCase: useCase,
	}
}

func (s *Server) GetPackages(ctx context.Context, req *pb.GetPackagesRequest) (*pb.GetPackagesResponse, error) {
	if req.GetFrom() == nil || req.GetTo() == nil {
		return nil, status.Error(codes.InvalidArgument, "from and to are required")
	}

	if len(req.GetIds()) > query.MaxIDs {
		return nil, status.Errorf(codes.InvalidArgument, "too many ids, max %d", query.MaxIDs)
	}

	ids := make([]uuid.UUID, 0, len(req.GetIds()))
	for _, rawID := range req.GetIds() {
		id, err := uuid.Parse(rawID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid id %q", rawID)
		}
		ids = append(ids, id)
	}
	packages, err := s.useCase.GetPackages(ctx, ids, req.GetFrom().AsTime(), req.GetTo().AsTime())
	if err != nil {
		return nil, s.toStatus(err)
	}
	resp := &pb.GetPackagesResponse{
		Packages: make([]*pb.DataPackage, 0, len(packages)),
	}
	for _, pkg := range packages {
		resp.Packages = append(resp.Packages, &pb.DataPackage{
			Id:        pkg.ID.String(),
			CreatedAt: timestamppb.New(pkg.CreatedAt),
			Payload:   int64(pkg.Payload),
		})
	}
	return resp, nil
}

func (s *Server) toStatus(err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	default:
		s.log.Error("grpc request failed", "error", err)
		return status.Error(codes.Internal, "internal error")
	}
}
