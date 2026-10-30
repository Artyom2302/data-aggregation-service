# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/data-aggregation-service ./cmd/data-aggregation-service

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app

COPY --from=build /out/data-aggregation-service ./
COPY config ./config
COPY migrations ./migrations

ENV CONFIG_PATH=/app/config/config.yaml
EXPOSE 8082 9090

ENTRYPOINT ["/app/data-aggregation-service"]
