# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements file first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application code
COPY *.py .

# Copy environment configuration
COPY .env .

# Set environment variable for Python
ENV PYTHONUNBUFFERED=1

# Run the ingestion worker
CMD ["python", "ingestion_worker.py"]