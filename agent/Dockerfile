FROM node:20-slim

# Install Python + pip for skills scripts, plus common tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

# Install Claude Code CLI globally
RUN npm install -g @anthropic-ai/claude-code

# Install Python dependencies for skills
WORKDIR /app/skills
COPY skills-requirements.txt /app/skills/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /app/skills/requirements.txt

# Create non-root user (required for --dangerously-skip-permissions)
RUN useradd -m -s /bin/bash agent
USER agent
WORKDIR /app

# Keep container alive for interactive `docker exec` usage
CMD ["tail", "-f", "/dev/null"]
