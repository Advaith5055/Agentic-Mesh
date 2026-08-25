FROM node:22-slim

# Install build dependencies for better-sqlite3 native compilation
RUN apt-get update && apt-get install -y \
    python3 \
    make \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency manifests
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy application source code
COPY . .

# Create data directory for SQLite database
RUN mkdir -p /app/data

# Default environment variables
ENV NODE_ENV=production
ENV P2P_PORT=9001
ENV WS_PORT=3001
ENV DB_PATH=/app/data/mesh.db
ENV OLLAMA_HOST=http://ollama:11434
ENV OLLAMA_MODEL=gemma3:4b

# Expose P2P port and WS gateway port
EXPOSE 9001 3001

CMD ["node", "index.js"]
