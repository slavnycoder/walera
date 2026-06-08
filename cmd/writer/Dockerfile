# syntax=docker/dockerfile:1.7
#
# Walera writer — testbench load generator (Phase 07 / Plan 07-03).
#
# Multi-stage build mirroring the root production Dockerfile pattern, but
# parameterised for the writer binary (./cmd/writer) and port 9100. The
# writer is testbench-only — there is NO production deployment of this
# image — but it shares the same security/runtime posture as the walera
# image so the testbench substrate is uniform:
#
#   Stage 1: golang:1.26.4-alpine → CGO-disabled static binary.
#   Stage 2: gcr.io/distroless/static-debian12:nonroot → ships
#            ca-certificates, /etc/passwd, /etc/group, tzdata; runs as
#            UID 65532 (nonroot:nonroot).
#
# IMPORTANT — build context MUST be the repo root, not cmd/writer/. The
# COPY paths below reference cmd/ + internal/ + go.mod + go.sum which
# only exist at the repo root. testbench/docker-compose.yml accordingly
# uses `build: { context: .., dockerfile: cmd/writer/Dockerfile }`.
#
# Build (from repo root):
#   docker build -t walera-testbench-writer:local -f cmd/writer/Dockerfile .
#
# Target image size: ≤ 25 MB (same target as the production walera image).
#
# DO NOT use `FROM scratch` — distroless/static-debian12 ships the minimum
# runtime files (ca-certificates for any future outbound HTTPS, /etc/passwd
# + /etc/group so the nonroot user resolves, tzdata) — see RESEARCH
# anti-pattern. The writer doesn't currently need ca-certificates but the
# uniform base posture matches the walera image.
#
# DO NOT use curl/wget for HEALTHCHECK — distroless has no shell, no curl,
# no wget. The HEALTHCHECK below uses the binary's own `-healthcheck` flag
# (defined in cmd/writer/main.go) which performs a localhost GET against
# /healthz and exits 0/1 accordingly. Mirrors the cdc-sse Phase 06 pattern.

# WR-04 fix: pin base images by SHA256 digest for reproducibility +
# supply-chain audit. Update quarterly via:
#   docker pull golang:1.26.4-alpine && \
#     docker inspect --format='{{index .RepoDigests 0}}' golang:1.26.4-alpine
# (golang digest re-resolved 2026-06-08 against docker.io/library; distroless
# runtime digest unchanged from the 2026-05-25 resolution).
FROM golang:1.26.4-alpine@sha256:f23e8b227fb4493eabe03bede4d5a32d04092da71962f1fb79b5f7d1e6c2a17f AS build
WORKDIR /src

# Cache go.mod / go.sum as a separate layer so source-only edits don't bust
# the module download cache.
COPY go.mod go.sum ./
RUN go mod download

# Now bring in the source. cmd/ + internal/ only — the repo-root
# .dockerignore additionally excludes test/, deploy/, *.md, planning/, etc.
COPY cmd ./cmd
COPY internal ./internal

ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -trimpath \
    -o /out/writer \
    ./cmd/writer

# --- Runtime stage: distroless static, nonroot user.
# WR-04 fix: digest pin (see build-stage comment for refresh procedure).
FROM gcr.io/distroless/static-debian12:nonroot@sha256:a9329520abc449e3b14d5bc3a6ffae065bdde0f02667fa10880c49b35c109fd1
COPY --from=build /out/writer /writer
USER nonroot:nonroot
EXPOSE 9100

# Image-level healthcheck (defense-in-depth alongside the compose-level
# healthcheck in testbench/docker-compose.yml). CMD exec form — no shell
# available on distroless. The writer binary's `-healthcheck` flag GETs
# http://127.0.0.1:9100/healthz with a 2s timeout and exits 0/1.
HEALTHCHECK --interval=2s --timeout=2s --retries=15 --start-period=5s \
    CMD ["/writer", "-healthcheck"]

# No CMD — writer reads its config from flags / WRITER_* env vars only
# (no YAML config file expected on the testbench). All knobs are set via
# the compose `environment:` block.
ENTRYPOINT ["/writer"]
