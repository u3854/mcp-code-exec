# Use a lightweight Python base
FROM python:3.13-slim

# Prevent Python from writing pyc files and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system utilities (optional, but good for debugging)
# We clean up apt cache to keep image small
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user and group
RUN groupadd -r mcp-group && useradd -r -g mcp-group mcp-user

# Set the working directory
WORKDIR /app

# 1. Install Dependencies first (Caching layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Setup File System & Sandbox
# Create the sandbox directory
RUN mkdir -p /app/sandbox

# Copy application code
COPY . /app

# 3. Permissions
# Give mcp-user ownership of the app directory
# Crucial: This ensures it can write to /app/sandbox
RUN chown -R mcp-user:mcp-group /app

# Switch to non-root user
USER mcp-user

# Expose port (if using HTTP/SSE transport)
EXPOSE 8000

# Entrypoint
# By default, FastMCP runs in stdio mode. 
# You can pass args to this command to switch to SSE if needed.
CMD ["fastmcp", "run", "server.py:mcp", "--port", "8000", "--transport", "http", "--host", "0.0.0.0"]