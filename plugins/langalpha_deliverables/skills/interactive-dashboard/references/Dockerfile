FROM python:3.12-slim
ENV PORT=8050

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl nodejs npm tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && pip install uv

WORKDIR /app

# Backend deps via uv (10-100x faster than pip, cached layer)
COPY server/requirements.txt ./
RUN uv pip install --system --no-cache -r requirements.txt

# Frontend build (cached layer — only rebuilds when package.json changes)
COPY frontend/package.json frontend/package-lock.json* ./frontend/
RUN cd frontend && npm install --prefer-offline
COPY frontend/ ./frontend/
RUN cd frontend && npm run build

# Backend code — serves frontend static from build output
COPY server/ ./server/

EXPOSE ${PORT}
CMD uvicorn server.main:app --host 0.0.0.0 --port ${PORT}
