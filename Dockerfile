# syntax=docker/dockerfile:1.7
#
# Walera — PostgreSQL CDC over SSE.
#
# Multi-stage build per RESEARCH Pattern 11 / D4-21:
#   Stage 1: golang:1.25.10-alpine → static binary with CGO disabled.
#   Stage 2: gcr.io/distroless/static-debian12:nonroot → ca-certificates,
#            /etc/passwd, /etc/group, timezone db — everything a static Go
#            binary actually needs. Runs as UID 65532 (nonroot:nonroot).
#
# Build: docker build -t walera:dev .
# Optional VERSION baked in via -X main.version:
#   docker build --build-arg VERSION=v1.0.0 -t walera:v1.0.0 .
#
# Target image size: ≤ 25 MB.
#
# DO NOT use `FROM scratch` — lacks ca-certificates so the auth-backend HTTPS
# call would fail. distroless/static-debian12 ships the minimum runtime files
# (ca-certificates, /etc/passwd, /etc/group, tzdata) — see RESEARCH anti-pattern.

FROM golang:1.25.10-alpine AS build
WORKDIR /src

# Cache go.mod / go.sum as a separate layer so source-only edits don't bust
# the module download cache.
COPY go.mod go.sum ./
RUN go mod download

# Now bring in the source. Only the production-binary tree — cmd/ + internal/.
# The .dockerignore additionally excludes test/, deploy/, *.md, etc.
COPY cmd ./cmd
COPY internal ./internal

ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -trimpath \
    -o /out/cdc-sse \
    ./cmd/cdc-sse

# --- Runtime stage: distroless static, nonroot user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/cdc-sse /cdc-sse
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/cdc-sse"]
CMD ["--config", "/etc/walera/config.yaml"]
