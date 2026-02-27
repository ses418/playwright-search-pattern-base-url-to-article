# ==========================================================
# Article Scraper API — FastAPI + Playwright
# Production-Ready (Coolify / Ubuntu Compatible)
# ==========================================================

# Official Microsoft Playwright image (Ubuntu Jammy based)
FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

# Prevent Python buffering
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy requirements first (better Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose FastAPI port
EXPOSE 5060

# Start FastAPI
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5060"]