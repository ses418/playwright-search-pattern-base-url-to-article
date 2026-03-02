# Use official Playwright image (already includes browsers + deps)
FROM mcr.microsoft.com/playwright/python:v1.43.0-jammy

WORKDIR /app

# Copy only requirements first (better Docker caching)
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy rest of the project
COPY . .

# Expose your FastAPI port
EXPOSE 5070

# Start app
CMD ["python", "-m", "app.main"]