# Use a lightweight Python base
FROM python:3.13-slim

# Prevent Python from writing pyc files and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# --- CHANGE 1: Create User & Fix Permissions ---
RUN groupadd -r mcp-group && useradd -r -g mcp-group mcp-user

# --- CHANGE 2: Set HOME explicitly ---
# Without this, 'pip install --user' doesn't know where to go, or defaults to /
ENV HOME=/home/mcp-user
ENV PATH="${HOME}/.local/bin:${PATH}"

# Create the home directory structure explicitly
RUN mkdir -p ${HOME}/.local/bin \
    && mkdir -p ${HOME}/.cache \
    && chown -R mcp-user:mcp-group ${HOME} \
    && chmod -R 755 ${HOME}

WORKDIR /app

# Install Dependencies (System Level)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Setup Sandbox
RUN mkdir -p /app/sandbox \
    && mkdir -p /app/engine

# Copy application code
COPY . /app

# Permissions
RUN chown -R mcp-user:mcp-group /app

# Switch to non-root user
USER mcp-user

EXPOSE 8000

# Use the fastmcp CLI command
CMD ["fastmcp", "run", "server.py:mcp", "--transport", "http", "--port", "8000", "--host", "0.0.0.0"]