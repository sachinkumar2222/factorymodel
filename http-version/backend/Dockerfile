# ============================================================
# Stage 1: Build native dependencies (better-sqlite3)
# ============================================================
FROM node:22-bookworm-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    make \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm ci --omit=dev

# ============================================================
# Stage 2: Production runtime
# ============================================================
FROM node:22-bookworm-slim AS runner

WORKDIR /app

ENV NODE_ENV=production \
    PORT=4000 \
    DB_PATH=/app/data/factory_model.db

# Ensure data directory exists and has proper permissions
RUN mkdir -p /app/data && chown -R node:node /app

# Copy built node_modules and application files
COPY --from=builder --chown=node:node /app/node_modules ./node_modules
COPY --chown=node:node package*.json ./
COPY --chown=node:node server.js ./
COPY --chown=node:node public ./public

USER node

EXPOSE 4000

CMD ["node", "server.js"]
