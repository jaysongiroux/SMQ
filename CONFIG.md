# Configuration Documentation

This document describes all configuration options for SMQ (Scheduled Message Queue). Configuration can be provided through either a `config.json` file or environment variables.

**All configuration values can be overridden via environment variables.** When both `config.json` and environment variables are provided, environment variables take precedence.

---

## Configuration Priority

Configuration values are resolved in the following order (highest to lowest priority):

1. **Environment Variables** - Always override config.json values
2. **config.json** - Application defaults
3. **Built-in Defaults** - Hardcoded fallback values (where applicable)

**Example:** If `producer_port` is set in both `config.json` (8081) and as an environment variable (9000), the environment variable value (9000) will be used.

---

## Configuration Reference

### General Configuration

| Field / Variable | Type | Description | Required | Validation | Default |
|------------------|------|-------------|----------|------------|---------|
| `SMQ_CONFIG_PATH` | string | Path to the `config.json` file. Environment variable only. | No | Valid file path | `./config.json` |
| `node_id` | string | Unique identifier for this node instance. If not provided, a UUID is auto-generated on startup. | No | Any string | Auto-generated UUID |
| `api_key` | string | Authentication key for REST API requests. **Must be at least 32 characters for security.** | **Yes** | Min 32 characters | - |
| `datastore` | string | Database type. | **Yes** | `postgres` or `cockroach` | - |
| `region` | string | Deployment region identifier for multi-region configurations. Used for data locality in CockroachDB. PostgreSQL ignores this setting. | **Yes** | Non-empty string | - |
| `log_level` | string | Logging verbosity level. | **Yes** | `debug`, `info`, `warn`, or `error` | - |

### Database Configuration

Configure the appropriate section based on your chosen `datastore` value.

#### PostgreSQL Configuration

| Field / Variable | Type | Description | Required | Validation | Default |
|------------------|------|-------------|----------|------------|---------|
| `postgres_url` | string | PostgreSQL database connection string (DSN). | **Yes** when `datastore=postgres` | Non-empty string | - |
| `postgres_max_open_conns` | integer | Maximum number of open connections to the database. | **Yes** when `datastore=postgres` | 1-100 | - |
| `postgres_max_idle_conns` | integer | Maximum number of idle connections in the connection pool. | **Yes** when `datastore=postgres` | 1-100 | - |

#### CockroachDB Configuration

| Field / Variable | Type | Description | Required | Validation | Default |
|------------------|------|-------------|----------|------------|---------|
| `cockroach_url` | string | CockroachDB connection string (DSN). | **Yes** when `datastore=cockroach` | Non-empty string | - |
| `cockroach_region` | string | CockroachDB region name for multi-region deployments. Must match a configured region in your CockroachDB cluster. | **Yes** when `datastore=cockroach` | Non-empty string | - |
| `cockroach_max_open_conns` | integer | Maximum number of open connections to the database. | **Yes** when `datastore=cockroach` | 1-100 | - |
| `cockroach_max_idle_conns` | integer | Maximum number of idle connections in the connection pool. | **Yes** when `datastore=cockroach` | 1-100 | - |

---

## Operational Configuration

The following sections configure runtime behavior. All fields can be set in `config.json` or as environment variables.

### Scheduler Configuration

Controls the behavior of background scheduler processes that manage message lifecycle.

| Field / Variable | Type | Description | Required | Validation | Default |
|------------------|------|-------------|----------|------------|---------|
| `num_scheduler_nodes` | integer | Number of concurrent scheduler goroutines. Each scheduler independently marks pending messages as ready for processing. Increase for higher throughput. | **Yes** | Min: 1 | - |
| `num_scheduler_janitor_nodes` | integer | Number of concurrent janitor goroutines. Each janitor independently cleans up stale acquired messages and offline nodes. | **Yes** | Min: 1 | - |
| `scheduler_poll_interval_ms` | integer | Interval in milliseconds between scheduler polls to mark pending messages as ready. Lower values reduce delivery latency but increase database load. | **Yes** | Min: 500 | - |
| `scheduler_janitor_interval_ms` | integer | Interval in milliseconds between janitor cleanup runs for stale messages and nodes. | **Yes** | Min: 1000 | - |
| `scheduler_max_messages_per_poll` | integer | Maximum number of messages to process in a single scheduler poll. Higher values increase throughput but may cause longer poll times. | **Yes** | 100-1000000 | - |
| `scheduler_poll_jitter_percent` | integer | Random jitter percentage applied to scheduler poll intervals to prevent thundering herd. Prevents all scheduler nodes from polling simultaneously. | **Yes** | 5-100 | - |
| `scheduler_janitor_jitter_percent` | integer | Random jitter percentage applied to janitor intervals to distribute cleanup operations across nodes. | **Yes** | 5-100 | - |

### REST API Configuration

Each API layer runs as an independent HTTP server on its own port.

| Field / Variable | Type | Description | Required | Validation | Default |
|------------------|------|-------------|----------|------------|---------|
| `producer_port` | integer | Port for the producer API server (message submission endpoints). | **Yes** | 1-65535 | - |
| `consumer_port` | integer | Port for the consumer API server (message retrieval, ack/nack endpoints). | **Yes** | 1-65535 | - |
| `health_port` | integer | Port for the health check API server (cluster health monitoring). | **Yes** | 1-65535 | - |

### Message Handling Configuration

Controls message processing behavior and limits.

| Field / Variable | Type | Description | Required | Validation | Default |
|------------------|------|-------------|----------|------------|---------|
| `msg_timeout_ms` | integer | Timeout in milliseconds for acquired messages. If a message remains in `ACQUIRED` state longer than this duration without ack/nack, the janitor marks it as ready for retry. Set based on expected consumer processing time. | **Yes** | Min: 1000 | - |
| `max_retries` | integer | Maximum number of delivery attempts before moving a message to failed status. After exceeding this limit, messages are either deleted or moved to DLQ based on `janitor_delete_failed_messages` setting. | **Yes** | Min: 0 | - |
| `max_payload_size_kb` | integer | Maximum message payload size in kilobytes. Messages exceeding this size will be rejected at submission. Consider database storage limits when setting this value. | **Yes** | Min: 1 | - |
| `min_scheduled_at_future_ms` | integer | Minimum time in milliseconds that a scheduled message must be in the future relative to creation time. Prevents scheduling messages in the past or immediate present. | **Yes** | Min: 5000 | - |

### Health Check Configuration

Controls system health monitoring behavior.

| Field / Variable | Type | Description | Required | Validation | Default |
|------------------|------|-------------|----------|------------|---------|
| `health_check_interval_ms` | integer | Interval in milliseconds between health check polls of internal components (producer, consumer, buffer, scheduler, janitor). Health status is persisted to the database for cluster monitoring. Stale node cleanup threshold is `2 × health_check_interval_ms`. | **Yes** | Min: 1000 | - |

### Janitor Configuration

Controls cleanup behavior for failed messages and stale nodes.

| Field / Variable | Type | Description | Required | Validation | Default |
|------------------|------|-------------|----------|------------|---------|
| `janitor_delete_failed_messages` | boolean | When `true`, permanently deletes messages that exceed `max_retries`. When `false`, moves failed messages to Dead Letter Queue (DLQ) by appending `.DLQ` to the channel name for manual review. **Note:** DLQ messages persist indefinitely unless explicitly deleted via `DELETE /v1/message` or consumed from the DLQ channel. | **Yes** | boolean | - |

### Buffer Configuration

Controls the message buffer mechanism for write optimization. The buffer batches messages before writing to the database to reduce write amplification.

| Field / Variable | Type | Description | Required | Validation | Default |
|------------------|------|-------------|----------|------------|---------|
| `buffer_type` | string | Buffer implementation type. Options: `memory` (higher performance, volatile), `disk` (durable via write-ahead log, slightly lower performance). **Memory buffer:** Fastest but messages in buffer are lost on crash. **Disk buffer:** Uses WAL for durability, survives crashes, suitable for critical messages. | **Yes** | `memory` or `disk` | - |
| `buffer_wal_path` | string | File system path for the write-ahead log. **Required when `buffer_type` is `disk`**. The WAL file stores messages before database writes. Ensure sufficient disk space and write permissions. Example: `./smq_wal.log` or `/var/lib/smq/wal.log` | Conditional | Non-empty when `buffer_type=disk` | - |
| `buffer_flush_interval_ms` | integer | Interval in milliseconds between buffer flushes to the database. Lower values reduce potential data loss window but increase write frequency and database load. Balance based on durability vs. performance requirements. | **Yes** | Min: 1000 | - |
| `buffer_max_size_kb` | integer | Maximum size in kilobytes for messages to batch before forcing a flush. When reached, triggers immediate flush regardless of `buffer_flush_interval_ms`. | **Yes** | 10-1000000 | - |
| `num_buffer_nodes` | integer | Number of concurrent buffer worker goroutines for flushing batches. Increase for higher write throughput if database can handle parallel writes. | **Yes** | 1-100 | - |

### Multi-Region Configuration

Controls behavior for multi-region deployments (primarily for CockroachDB).

| Field / Variable | Type | Description | Required | Validation | Default |
|------------------|------|-------------|----------|------------|---------|
| `multi_region_supplement` | boolean | When `true`, allows supplementing local region messages with messages from other regions when polling. Useful for cross-region workload balancing. | **Yes** | boolean | - |
| `multi_region_scheduler` | boolean | When `true`, enables the scheduler to process messages from all regions, not just the local region. | **Yes** | boolean | - |
| `multi_region_janitor` | boolean | When `true`, enables the janitor to clean up messages from all regions, not just the local region. | **Yes** | boolean | - |

---

## Validation Rules

The application validates all configuration on startup. If validation fails, the application will not start and will print detailed error messages.

### Common Validation Errors

- **Missing required fields**: All fields marked as "Required: Yes" must be provided
- **Missing database credentials**: `postgres_url` or `cockroach_url` must be set based on `datastore`
- **Invalid port numbers**: Ports must be between 1 and 65535
- **Invalid log level**: Must be one of: `debug`, `info`, `warn`, `error`
- **Invalid datastore**: Must be `postgres` or `cockroach`
- **Invalid buffer type**: Must be `memory` or `disk`
- **Missing WAL path**: Required and non-empty when `buffer_type` is `disk`
- **Invalid jitter percentage**: Must be between 5 and 100
- **Invalid intervals**: Most intervals have minimum values (e.g., 1000ms for buffer flush)
- **API key too short**: Must be at least 32 characters for security
- **Out of range values**: Check validation column for each field's allowed range
- **Connection pool limits**: Max open/idle connections must be between 1 and 100

---

## Performance Tuning Guidelines

### High Throughput Scenarios

**Goal:** Maximize message processing rate

- **Buffer**:
  - Use `buffer_type: "memory"` for maximum speed
  - Set `buffer_max_size_kb: 5000-10000` for larger batches
  - Increase `num_buffer_nodes: 5-10` for parallel writes
  - Keep `buffer_flush_interval_ms: 1000-2000` for frequent flushes

- **Scheduler**:
  - Increase `num_scheduler_nodes: 3-5` for parallel processing
  - Lower `scheduler_poll_interval_ms: 500-1000` for faster message pickup
  - Increase `scheduler_max_messages_per_poll: 5000-10000` for bulk processing
  - Use `scheduler_poll_jitter_percent: 15-25` to distribute load

- **Database**:
  - Increase `postgres_max_open_conns: 50-100` or `cockroach_max_open_conns: 50-100`
  - Increase `postgres_max_idle_conns: 25-50` or `cockroach_max_idle_conns: 25-50`

### High Durability Scenarios

**Goal:** Minimize message loss and ensure reliability

- **Buffer**:
  - Use `buffer_type: "disk"` with WAL for crash recovery
  - Set `buffer_wal_path` to persistent storage
  - Use moderate `buffer_flush_interval_ms: 1000-3000`
  - Keep `num_buffer_nodes: 2-3` to avoid overwhelming database

- **Retries & Timeouts**:
  - Set `max_retries: 5-10` for multiple delivery attempts
  - Set `janitor_delete_failed_messages: false` to preserve failed messages in DLQ
  - Increase `msg_timeout_ms: 60000-300000` (1-5 minutes) based on processing time

- **Monitoring**:
  - Lower `health_check_interval_ms: 2000-3000` for faster failure detection
  - Enable DLQ monitoring to track failed messages

### Multi-Region Deployments (CockroachDB)

**Goal:** Global availability and low latency

- **Region Configuration**:
  - Set unique `region` value per deployment location (e.g., `us-east-1`, `eu-west-1`)
  - Use `datastore: "cockroach"` with proper `cockroach_region` setting
  - Enable `multi_region_supplement: true` for cross-region load balancing
  - Consider `multi_region_scheduler: true` and `multi_region_janitor: true` for global processing

- **Latency Considerations**:
  - Increase `msg_timeout_ms` to account for cross-region latency
  - Use higher `scheduler_poll_jitter_percent: 20-30` to prevent coordinated polls
  - Monitor database query latency and adjust `scheduler_poll_interval_ms` accordingly

- **Connection Pooling**:
  - Use higher connection limits: `cockroach_max_open_conns: 50-75`
  - Balance idle connections: `cockroach_max_idle_conns: 25-40`



## Security Best Practices

1. **Never commit `api_key` to version control** - use environment variables
2. **Never commit database credentials to version control** - use environment variables
3. **Use strong API keys** - minimum 32 characters, use cryptographically random values
4. **Rotate API keys regularly** - update environment variables and restart
5. **Use TLS/SSL for database connections** - include `sslmode=require` in connection strings
6. **Restrict file permissions on config.json** - `chmod 600 config.json` if it contains secrets
7. **Use separate credentials per environment** - dev, staging, production should have unique credentials
