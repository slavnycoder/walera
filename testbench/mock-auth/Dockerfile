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

# Healthcheck mandated by CONTEXT.md — uses urllib.request (no curl in alpine).
# _health channel honors FAIL_MODE on purpose: flipping FAIL_MODE flips
# container health, which is what makes Phase 06's breaker-trip demo work.
HEALTHCHECK --interval=2s --timeout=2s --retries=15 \
    CMD python -c "import urllib.request, sys; sys.exit(0 if urllib.request.urlopen('http://localhost:9000/auth/permissions?channel=_health', timeout=1).status == 200 else 1)"

CMD ["python", "-u", "server.py", "9000"]
