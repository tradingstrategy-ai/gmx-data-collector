FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY setup.py ./

# Install the package
RUN pip install --no-cache-dir -e .

# Create data and logs directories
RUN mkdir -p /app/data /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Entry point is the gmx_historical_data CLI
ENTRYPOINT ["gmx_historical_data"]

# Default command (help)
CMD ["--help"]
