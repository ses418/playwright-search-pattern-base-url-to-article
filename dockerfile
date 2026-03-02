# Use official Playwright image (already includes browsers + deps)
FROM mcr.microsoft.com/playwright/python:v1.58.0-jammy

WORKDIR /app

# Copy only requirements first (better Docker caching)
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy rest of the project
COPY . .

# Add a command to run the application with an environment file
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port 5070"]