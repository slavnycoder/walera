# testbench/mock-auth/Dockerfile
# Source: python:3.13-alpine official image; stdlib-only invocation.
# Walera v1.1 testbench mock-auth service (promoted from v1.0 smoke fixture).
FROM python:3.13-alpine

WORKDIR /app

# Copy script and declarative seed; no pip install — stdlib only.
COPY server.py seed.json /app/

# Defense in depth: drop root before exec. mock-auth binds 9000 (unprivileged)
# and writes no files after boot, so no root capabilities are needed.
RUN adduser -D -u 10001 mockauth
USER mockauth

EXPOSE 9000

# Healthcheck — GET /_health honors FAIL_MODE; flipping FAIL_MODE flips the
# container's docker-reported health (basis for Phase 06's breaker-trip demo).
# Auth-protected paths use HMAC; the public liveness endpoint does not.
HEALTHCHECK --interval=2s --timeout=2s --retries=15 \
    CMD python -c "import urllib.request, sys; sys.exit(0 if urllib.request.urlopen('http://localhost:9000/_health', timeout=1).status == 200 else 1)"

CMD ["python", "-u", "server.py", "9000"]
