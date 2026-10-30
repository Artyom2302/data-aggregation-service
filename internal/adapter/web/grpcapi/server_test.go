package grpcapi

import (
	"bytes"
	"context"
	pb "data-aggregation-service/gen/dataaggregation/v1"
	"data-aggregation-service/internal/domain"
	"data-aggregation-service/internal/usecase/query"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeUseCase struct {
	calls    int
	gotCtx   context.Context
	gotIDs   []uuid.UUID
	gotFrom  time.Time
	gotTo    time.Time
	packages []domain.DataPackage
	err      error
}

func (f *fakeUseCase) GetPackages(ctx context.Context, ids []uuid.UUID, from, to time.Time) ([]domain.DataPackage, error) {
	f.calls++
	f.gotCtx, f.gotIDs, f.gotFrom, f.gotTo = ctx, ids, from, to
	return f.packages, f.err
}

const testID = "5b1f8e0a-1111-4c3b-9f2a-000000000001"

var (
	from = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to   = time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
)

func newServer(uc *fakeUseCase, logOut io.Writer) *Server {
	return NewServer(slog.New(slog.NewTextHandler(logOut, nil)), uc)
}

func validRequest(ids ...string) *pb.GetPackagesRequest {
	return &pb.GetPackagesRequest{
		Ids:  ids,
		From: timestamppb.New(from),
		To:   timestamppb.New(to),
	}
}

func newIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = uuid.NewString()
	}
	return ids
}

// Запросы, которые адаптер должен отклонить сам, не вызывая use case.
func TestServer_GetPackages_Validation(t *testing.T) {
	tests := []struct {
		name    string
		req     *pb.GetPackagesRequest
		wantMsg string
	}{
		{"no from", &pb.GetPackagesRequest{Ids: []string{testID}, To: timestamppb.New(to)}, "from and to are required"},
		{"no to", &pb.GetPackagesRequest{Ids: []string{testID}, From: timestamppb.New(from)}, "from and to are required"},
		{"too many ids", validRequest(newIDs(query.MaxIDs + 1)...), "too many ids"},
		{"invalid uuid", validRequest(testID, "not-a-uuid"), `invalid id "not-a-uuid"`},
		{"empty id", validRequest(""), `invalid id ""`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := &fakeUseCase{}

			resp, err := newServer(uc, io.Discard).GetPackages(t.Context(), tt.req)

			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("code: want InvalidArgument, got %v (%v)", got, err)
			}
			if msg := status.Convert(err).Message(); !strings.Contains(msg, tt.wantMsg) {
				t.Errorf("message: want to contain %q, got %q", tt.wantMsg, msg)
			}
			if resp != nil {
				t.Errorf("want nil response, got %v", resp)
			}
			if uc.calls != 0 {
				t.Errorf("use case must not be called on invalid request, called %d times", uc.calls)
			}
		})
	}
}

func TestServer_GetPackages(t *testing.T) {
	createdAt := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		packages []domain.DataPackage
		want     *pb.GetPackagesResponse
	}{
		{
			name: "found",
			packages: []domain.DataPackage{
				{ID: uuid.MustParse(testID), CreatedAt: createdAt, Payload: 7},
				{ID: uuid.MustParse(testID), CreatedAt: createdAt.Add(time.Hour), Payload: -3},
			},
			want: &pb.GetPackagesResponse{Packages: []*pb.DataPackage{
				{Id: testID, CreatedAt: timestamppb.New(createdAt), Payload: 7},
				{Id: testID, CreatedAt: timestamppb.New(createdAt.Add(time.Hour)), Payload: -3},
			}},
		},
		{
			name: "nothing found",
			want: &pb.GetPackagesResponse{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := &fakeUseCase{packages: tt.packages}

			resp, err := newServer(uc, io.Discard).GetPackages(t.Context(), validRequest(testID))

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// Сообщения protobuf сравниваем только через proto.Equal: в них есть служебные поля.
			if !proto.Equal(resp, tt.want) {
				t.Errorf("response:\nwant %v\ngot  %v", tt.want, resp)
			}
		})
	}
}

func TestServer_GetPackages_RequestParsing(t *testing.T) {
	ids := newIDs(2)
	uc := &fakeUseCase{}

	if _, err := newServer(uc, io.Discard).GetPackages(t.Context(), validRequest(ids...)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if uc.calls != 1 {
		t.Fatalf("use case: want 1 call, got %d", uc.calls)
	}
	// Регрессия: раньше make с длиной вместо ёмкости добавлял нулевые UUID в начало.
	want := []uuid.UUID{uuid.MustParse(ids[0]), uuid.MustParse(ids[1])}
	if !slices.Equal(uc.gotIDs, want) {
		t.Errorf("ids: want %v, got %v", want, uc.gotIDs)
	}
	if !uc.gotFrom.Equal(from) || !uc.gotTo.Equal(to) {
		t.Errorf("range: want [%v, %v], got [%v, %v]", from, to, uc.gotFrom, uc.gotTo)
	}
}

func TestServer_GetPackages_UseCaseError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantCode   codes.Code
		hiddenText string
	}{
		{"invalid argument", fmt.Errorf("%w: from must not be after to", domain.ErrInvalidArgument), codes.InvalidArgument, ""},
		{"canceled", fmt.Errorf("query: %w", context.Canceled), codes.Canceled, ""},
		{"deadline exceeded", fmt.Errorf("query: %w", context.DeadlineExceeded), codes.DeadlineExceeded, ""},
		{"internal error", errors.New("db is down"), codes.Internal, "db is down"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := &fakeUseCase{err: tt.err}
			var logs bytes.Buffer

			_, err := newServer(uc, &logs).GetPackages(t.Context(), validRequest(testID))

			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code: want %v, got %v (%v)", tt.wantCode, got, err)
			}
			if tt.hiddenText == "" {
				return
			}
			if msg := status.Convert(err).Message(); strings.Contains(msg, tt.hiddenText) {
				t.Errorf("internal error leaked to client: %q", msg)
			}
			if !strings.Contains(logs.String(), tt.hiddenText) {
				t.Errorf("internal error must be logged, logs: %s", logs.String())
			}
		})
	}
}

func newTestClient(t *testing.T, uc *fakeUseCase) pb.DataAggregationServiceClient {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterDataAggregationServiceServer(srv, newServer(uc, io.Discard))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return pb.NewDataAggregationServiceClient(conn)
}

func TestServer_GetPackages_OverGRPC(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		createdAt := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
		uc := &fakeUseCase{packages: []domain.DataPackage{{ID: uuid.MustParse(testID), CreatedAt: createdAt, Payload: 7}}}
		client := newTestClient(t, uc)

		resp, err := client.GetPackages(t.Context(), validRequest(testID))

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := &pb.GetPackagesResponse{Packages: []*pb.DataPackage{
			{Id: testID, CreatedAt: timestamppb.New(createdAt), Payload: 7},
		}}
		if !proto.Equal(resp, want) {
			t.Errorf("response:\nwant %v\ngot  %v", want, resp)
		}
	})

	t.Run("error code reaches client", func(t *testing.T) {
		client := newTestClient(t, &fakeUseCase{})

		_, err := client.GetPackages(t.Context(), &pb.GetPackagesRequest{Ids: []string{testID}})

		if got := status.Code(err); got != codes.InvalidArgument {
			t.Errorf("code: want InvalidArgument, got %v (%v)", got, err)
		}
	})

	t.Run("client deadline reaches use case", func(t *testing.T) {
		uc := &fakeUseCase{}
		client := newTestClient(t, uc)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		if _, err := client.GetPackages(ctx, validRequest(testID)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if _, ok := uc.gotCtx.Deadline(); !ok {
			t.Error("use case context must carry the client's deadline")
		}
	})
}
