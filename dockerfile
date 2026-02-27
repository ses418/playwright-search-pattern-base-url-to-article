# ----------------------------
# Base Image
# ----------------------------
FROM python:3.11-slim

# ----------------------------
# Set working directory
# ----------------------------
WORKDIR /app

# ----------------------------
# Install system dependencies required for Playwright
# ----------------------------
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    gnupg \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    libatspi2.0-0 \
    libxshmfence1 \
    libdrm2 \
    && rm -rf /var/lib/apt/lists/*

# ----------------------------
# Copy project files
# ----------------------------
COPY . .

# ----------------------------
# Install Python dependencies
# ----------------------------
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# ----------------------------
# Install Playwright browsers
# ----------------------------
RUN playwright install --with-deps chromium

# ----------------------------
# Expose FastAPI port
# ----------------------------
EXPOSE 5070

# ----------------------------
# Run the application
# ----------------------------
CMD ["python", "-m", "app.main"]