FROM node:20-slim

# Install Python + pip for skills scripts, plus common tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

# Open-source Codex CLI; the selected hosted model is a separate service.
ARG CODEX_CLI_VERSION=0.163.0
RUN npm install -g @openai/codex@${CODEX_CLI_VERSION}

# Install Python dependencies for skills
WORKDIR /app/skills
COPY skills-requirements.txt /app/skills/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /app/skills/requirements.txt

# Independent writable work/auth/run directories, with no host credential mount.
RUN useradd -m -s /bin/bash agent \
    && mkdir -p /app/runs /home/agent/.codex \
    && chown -R agent:agent /app /home/agent/.codex
COPY AGENTS.md /app/AGENTS.md
COPY codex_runner.py /app/codex_runner.py
COPY role_policy.py /app/role_policy.py
ENV CODEX_HOME=/home/agent/.codex
USER agent
WORKDIR /app

# Keep container alive for interactive `docker exec` usage
CMD ["tail", "-f", "/dev/null"]
