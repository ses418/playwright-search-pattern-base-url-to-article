# ═══════════════════════════════════════════════════════════════════════════════
#  Shared Dockerfile — Monorepo
#  Works for BOTH:
#    - scraper_service/   (your folder)
#    - colleague_service/ (colleague's folder)
#
#  Usage:
#    # Build your service:
#    docker build --build-arg SERVICE=scraper_service -t scraper-api .
#
#    # Build colleague's service:
#    docker build --build-arg SERVICE=colleague_service -t colleague-api .
#
#  The ARG SERVICE tells Docker which subfolder to copy and run.
#  Each subfolder must contain:  main.py  requirements.txt
# ═══════════════════════════════════════════════════════════════════════════════

ARG SERVICE=scraper_service

# ─── Stage 1: Python base ─────────────────────────────────────────────────────
FROM python:3.11-slim AS base

# Install system deps needed by Playwright Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl gnupg ca-certificates \
    # Chromium runtime deps
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libasound2 libpango-1.0-0 libpangocairo-1.0-0 \
    libx11-6 libx11-xcb1 libxcb1 libxext6 libxss1 libxtst6 \
    fonts-liberation libappindicator3-1 lsb-release xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# ─── Stage 2: Install Python deps ─────────────────────────────────────────────
FROM base AS deps

ARG SERVICE
WORKDIR /app

# Copy only the requirements for the target service (layer cache friendly)
COPY ${SERVICE}/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium browser
RUN playwright install chromium \
 && playwright install-deps chromium

# ─── Stage 3: Final runtime image ─────────────────────────────────────────────
FROM deps AS runtime

ARG SERVICE
WORKDIR /app

# Copy only the target service's code
COPY ${SERVICE}/ ./

# Create non-root user for security
RUN useradd -m -u 1001 appuser \
 && chown -R appuser:appuser /app

USER appuser

# Port exposed by the API
EXPOSE 5060

# Healthcheck — hits /health endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD wget -qO- http://localhost:5060/health || exit 1

# Run the FastAPI server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5060", "--workers", "1"]