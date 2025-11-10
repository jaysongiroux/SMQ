# Docker Deployment Guide

This guide covers deploying SMQ using Docker for production environments.

## Quick Start with Docker Compose

The fastest way to get started is using Docker Compose, which includes both SMQ and PostgreSQL:

```bash
# Start the stack
docker-compose up -d

# View logs
docker-compose logs -f smq

# Stop the stack
docker-compose down
```

**Important:** Change the `api_key` in `docker-compose.yml` before deploying to production!

---

## Building the Docker Image

### Build Locally

```bash
# Build the image
docker build -t smq:latest .

# Verify the image size (should be ~20-30MB)
docker images smq:latest
```

### Multi-Platform Build (Optional)

```bash
# Build for multiple architectures
docker buildx build --platform linux/amd64,linux/arm64 -t smq:latest .
```

---

## Running SMQ Container

### Prerequisites

1. **Database**: A running PostgreSQL instance (or other supported database)
2. **Configuration**: A valid `config.json` file
3. **Environment Variables**: Database credentials and API key

### Basic Run Command

```bash
docker run -d \
  --name smq \
  -p 8081:8081 \
  -p 8082:8082 \
  -p 8083:8083 \
  -v $(pwd)/config.json:/config/config.json:ro \
  -v smq_data:/data \
  -e datastore=postgres \
  -e postgres_host=your-db-host \
  -e postgres_port=5432 \
  -e postgres_db=smq \
  -e postgres_user=smq \
  -e postgres_password=your-password \
  -e api_key=your-secure-32-character-or-longer-api-key \
  smq:latest
```

### Environment Variables

All configuration values can be overridden via environment variables:

**Required:**
- `datastore`: Database type (postgres, cockroach)
- `api_key`: API authentication key (minimum 32 characters)
- Database credentials (see CONFIG.md for database-specific variables)

**Optional:**
- `producer_port`: Override producer API port
- `consumer_port`: Override consumer API port
- `health_port`: Override health API port
- `log_level`: Override log level (debug, info, warn, error)
- `buffer_type`: Override buffer type (memory, disk)
- Any other config.json value (see CONFIG.md)

---

## Volume Mounts

### Configuration File

Mount your `config.json` as read-only:

```bash
-v /path/to/config.json:/config/config.json:ro
```

The container expects the config at `/config/config.json` by default. Override with:

```bash
-e SMQ_CONFIG_PATH=/config/custom-config.json
```

### Data Directory (WAL)

If using disk buffer, mount a persistent volume for the WAL:

```bash
-v smq_data:/data
```

Default WAL path is `/data/smq_wal.log`. Override with:

```bash
-e buffer_wal_path=/data/custom-wal.log
```

---

## Production Deployment

### Security Best Practices

1. **API Key**: Use a strong, randomly generated key (at least 32 characters)
   ```bash
   # Generate a secure API key
   openssl rand -hex 32
   ```

2. **Non-Root User**: The container runs as user `smq` (UID 1000) by default

3. **Read-Only Filesystem**: Mount config.json as read-only (`:ro`)

4. **Network Isolation**: Use Docker networks to isolate database traffic
   ```bash
   docker network create smq-network
   ```

5. **Resource Limits**: Set memory and CPU limits
   ```bash
   docker run --memory="512m" --cpus="1.0" ...
   ```

### High Availability Setup

For production HA deployments:

1. **Multiple Instances**: Run multiple SMQ containers behind a load balancer
2. **Shared Database**: All instances connect to the same database cluster
3. **Node Coordination**: SMQ handles distributed coordination automatically
4. **Health Checks**: Monitor `/v1/health` endpoint

Example with 3 instances:

```bash
# Instance 1
docker run -d --name smq-node-1 -p 8081:8081 -p 8082:8082 -p 8083:8083 ...

# Instance 2
docker run -d --name smq-node-2 -p 8091:8081 -p 8092:8082 -p 8093:8083 ...

# Instance 3
docker run -d --name smq-node-3 -p 8101:8081 -p 8102:8082 -p 8103:8083 ...
```

### Resource Requirements

**Minimum:**
- Memory: 256MB
- CPU: 0.5 cores
- Disk: 100MB + WAL space (if disk buffer)

**Recommended (Production):**
- Memory: 512MB - 1GB
- CPU: 1-2 cores
- Disk: 1GB + WAL space

### Monitoring

Health check endpoint:

```bash
curl -H "api-key: your-key" http://localhost:8083/v1/health
```

Container logs:

```bash
# Follow logs
docker logs -f smq

# Last 100 lines
docker logs --tail 100 smq

# With timestamps
docker logs -t smq
```

---

## Configuration Override Examples

### Override Ports

```bash
docker run -d \
  -e producer_port=9081 \
  -e consumer_port=9082 \
  -e health_port=9083 \
  -p 9081:9081 \
  -p 9082:9082 \
  -p 9083:9083 \
  ...
```

### Use Memory Buffer

```bash
docker run -d \
  -e buffer_type=memory \
  ...
```

### Use Disk Buffer

```bash
docker run -d \
  -e buffer_type=disk \
  -e buffer_wal_path=/data/smq_wal.log \
  -v smq_data:/data \
  ...
```

### High Throughput Configuration

```bash
docker run -d \
  -e buffer_type=memory \
  -e buffer_max_size=1000 \
  -e num_scheduler_nodes=4 \
  -e scheduler_poll_interval_ms=500 \
  --memory="1g" \
  --cpus="2.0" \
  ...
```

### High Durability Configuration

```bash
docker run -d \
  -e buffer_type=disk \
  -e buffer_flush_interval_ms=1000 \
  -e max_retries=10 \
  -e janitor_delete_failed_messages=false \
  -v smq_data:/data \
  ...
```

---

## Troubleshooting

### Container Won't Start

Check logs for configuration errors:

```bash
docker logs smq
```

Common issues:
- Missing required environment variables (database credentials, api_key)
- Invalid config.json format
- Database connection failures
- Port conflicts

### Database Connection Issues

Test database connectivity:

```bash
# From host
psql -h localhost -U smq -d smq

# From container network
docker run --rm --network container:smq postgres:16-alpine \
  psql -h postgres_host -U smq -d smq
```

### WAL File Growing

If using disk buffer and WAL file grows excessively:

1. Check database connectivity
2. Increase `buffer_flush_interval_ms`
3. Add more buffer workers: `-e buffer_worker_count=5`
4. Monitor database performance

### Permission Denied Errors

Ensure volumes have correct permissions:

```bash
# Create data directory with correct ownership
mkdir -p ./data
chown -R 1000:1000 ./data

# Mount with correct permissions
docker run -v $(pwd)/data:/data ...
```

### Health Check Failures

Check health endpoint manually:

```bash
docker exec smq wget -q -O- http://localhost:8083/v1/health
```

If health check fails:
1. Verify ports are correct
2. Check API key is set
3. Ensure database is reachable
4. Review application logs

---

## Kubernetes Deployment

Example Kubernetes manifests:

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: smq-config
data:
  config.json: |
    {
      "num_scheduler_nodes": 2,
      "producer_port": 8081,
      "consumer_port": 8082,
      "health_port": 8083,
      ...
    }
```

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: smq
spec:
  replicas: 3
  selector:
    matchLabels:
      app: smq
  template:
    metadata:
      labels:
        app: smq
    spec:
      containers:
      - name: smq
        image: smq:latest
        ports:
        - containerPort: 8081
          name: producer
        - containerPort: 8082
          name: consumer
        - containerPort: 8083
          name: health
        env:
        - name: datastore
          value: postgres
        - name: postgres_host
          valueFrom:
            secretKeyRef:
              name: smq-db-secret
              key: host
        - name: postgres_password
          valueFrom:
            secretKeyRef:
              name: smq-db-secret
              key: password
        - name: api_key
          valueFrom:
            secretKeyRef:
              name: smq-api-secret
              key: api-key
        volumeMounts:
        - name: config
          mountPath: /config
          readOnly: true
        - name: data
          mountPath: /data
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /v1/health
            port: 8083
            httpHeaders:
            - name: api-key
              value: your-api-key
          initialDelaySeconds: 10
          periodSeconds: 30
      volumes:
      - name: config
        configMap:
          name: smq-config
      - name: data
        persistentVolumeClaim:
          claimName: smq-data-pvc
```

---

## Image Details

### Base Image

- **Build Stage**: `golang:1.21-alpine` (~300MB)
- **Runtime Stage**: `alpine:3.19` (~7MB base)
- **Final Image Size**: ~20-30MB (compressed)

### Included Components

- SMQ binary (statically compiled)
- CA certificates (for HTTPS)
- Timezone data

### Security Features

- Runs as non-root user (UID 1000)
- Minimal attack surface (Alpine Linux)
- No shell or package manager in runtime
- Read-only root filesystem compatible
- No secrets in image layers

### Exposed Ports

- `8081`: Producer API (configurable)
- `8082`: Consumer API (configurable)
- `8083`: Health API (configurable)

### Health Check

The container includes a built-in health check that polls the `/v1/health` endpoint every 30 seconds.

---

## Support

For issues or questions:
1. Check application logs: `docker logs smq`
2. Review CONFIG.md for configuration options
3. Verify database connectivity
4. Check GitHub issues

