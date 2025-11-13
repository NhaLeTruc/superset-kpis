# Spark Development Environment with Testing Tools
FROM apache/spark:3.5.0-python3

# Switch to root to install packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create directories for project
RUN mkdir -p /app/src /app/tests /app/data

# Set PYTHONPATH to include /app
ENV PYTHONPATH=/app:$PYTHONPATH

# Switch back to spark user for security
USER spark

# Default command runs bash
CMD ["/bin/bash"]
