# ==========================================================
# Article Scraper API — FastAPI + Playwright
# ==========================================================

FROM python:3.12-slim

# Prevent Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1

# Install system dependencies required by Playwright
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    gnupg \
    libnss3 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libxkbcommon0 \
    libgtk-3-0 \
    libgbm1 \
    libasound2 \
    libxshmfence1 \
    fonts-liberation \
    libappindicator3-1 \
    libu2f-udev \
    xdg-utils \
    ca-certificates \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Set working directory inside container
WORKDIR /app

# Copy requirements first (better layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (Chromium only)
RUN playwright install chromium

# Copy entire project
COPY . .

# Expose FastAPI port
EXPOSE 5060

# Start FastAPI server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5060"]