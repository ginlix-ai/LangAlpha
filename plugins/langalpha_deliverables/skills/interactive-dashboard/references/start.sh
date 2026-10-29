#!/bin/bash
set -eo pipefail
cd "$(dirname "$0")"

PORT=${PORT:-8050}
NAME=${NAME:-dashboard}

# Skip restart if container is already healthy
if docker inspect "$NAME" &>/dev/null && curl -sf "http://127.0.0.1:$PORT/" &>/dev/null; then
  echo "Container already healthy on port $PORT — skipping restart"
  exit 0
fi

# Start dockerd if not running (needed after workspace restart)
if ! docker info &>/dev/null; then
  echo "Starting Docker daemon..."
  dockerd &>/tmp/dockerd.log &
  for i in $(seq 1 10); do docker info &>/dev/null && break || sleep 1; done
fi

echo "Building image..."
docker build -t "$NAME" .

echo "Starting container on port $PORT..."
docker rm -f "$NAME" 2>/dev/null || true
sleep 1
docker run -d --name "$NAME" --network host "$NAME"

echo "Waiting for server on port $PORT..."
for i in $(seq 1 30); do
  curl -sf "http://127.0.0.1:$PORT/" &>/dev/null && { echo "Ready on port $PORT"; exit 0; } || sleep 1
done
echo "Failed to start. Logs:" && docker logs "$NAME" && exit 1
