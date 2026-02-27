FROM python:3.11-slim

# ─── System deps ──────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl gnupg ca-certificates \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libasound2 libpango-1.0-0 libpangocairo-1.0-0 \
    libx11-6 libx11-xcb1 libxcb1 libxext6 libxss1 libxtst6 \
    fonts-liberation libappindicator3-1 lsb-release xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# ─── Set workdir & copy service ───────────────────────────────────────────────
WORKDIR /app

COPY base_url_to_article/ ./

# ─── Install Python dependencies ──────────────────────────────────────────────
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# ─── Install Playwright Chromium ──────────────────────────────────────────────
RUN playwright install chromium \
 && playwright install-deps chromium

# ─── Non-root user for security ───────────────────────────────────────────────
RUN useradd -m -u 1001 appuser \
 && chown -R appuser:appuser /app

USER appuser

# ─── Port & healthcheck ───────────────────────────────────────────────────────
EXPOSE 5060

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD wget -qO- http://localhost:5060/health || exit 1

# ─── Start server ─────────────────────────────────────────────────────────────
CMD ["uvicorn", "base_url_to_article.main:app", "--host", "0.0.0.0", "--port", "5060", "--workers", "1"]

