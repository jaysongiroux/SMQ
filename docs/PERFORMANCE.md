# Why Multiple Scheduler Routines Matter for Scaling

The scheduler is responsible for transitioning messages from `PENDING` to `READY` status when their `scheduled_at` time arrives. With a single scheduler routine, this becomes a bottleneck at high throughput.

**Key Benefits of Multiple Scheduler Routines:**

1. **Parallel Processing**: Multiple scheduler routines can process different batches of pending messages simultaneously. With `FOR UPDATE SKIP LOCKED`, each scheduler locks and processes a distinct set of messages without blocking others.

2. **Higher Throughput**: If a single scheduler can process 1,000 messages per poll at 100ms intervals (10,000 msg/sec), running 5 scheduler routines can theoretically process 50,000 msg/sec. The actual throughput scales linearly up to your database's capacity.

3. **Contention-Free Operation**: The optimized query using `FOR UPDATE SKIP LOCKED` ensures that when multiple schedulers query the same pending message batch, they never block each other. Each scheduler automatically skips locked rows and processes the next available messages.

4. **Fault Tolerance**: If one scheduler routine experiences a transient issue (timeout, temporary network blip), other schedulers continue processing. This prevents a single point of failure from stalling the entire queue.

5. **Reduced Latency at Scale**: With millions of pending messages scheduled far in the future, a single scheduler might take seconds to scan and process a large batch. Multiple schedulers divide this work, ensuring that messages scheduled to be ready "now" are marked promptly.

**Example Scenario:**

You have 10 million messages scheduled over the next 24 hours (average 115 msg/sec). At any given moment, 1,000-5,000 messages might be ready to transition from `PENDING` to `READY`.

- **1 scheduler routine** (1,000 msg/poll @ 100ms): Processes 1,000 messages every 100ms, but may fall behind during traffic spikes.
- **5 scheduler routines** (5,000 msg/poll combined @ 100ms): Each routine grabs 1,000 messages using `SKIP LOCKED`, processing 5,000 messages per cycle with no lock contention.

**Configuration Recommendations:**

- **Low-Moderate Throughput** (<10,000 msg/sec): `num_scheduler_nodes: 1-2`
- **High Throughput** (10,000-50,000 msg/sec): `num_scheduler_nodes: 3-5`
- **Very High Throughput** (>50,000 msg/sec): `num_scheduler_nodes: 5-10`

Always pair with appropriate jitter (`scheduler_poll_jitter_percent: 15-25`) to prevent synchronized polling across routines.

# Performance Tuning Guidelines

## High Throughput Scenarios

**Goal:** Maximize message processing rate

- **Buffer**:

  - Use `buffer_type: "memory"` for maximum speed
  - **Enable adaptive flushing:** `buffer_adaptive: true`
  - Set `buffer_adaptive_max_size: 10000` for large adaptive ceiling
  - Set `buffer_adaptive_min_size: 100` for reasonable floor
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

## High Durability Scenarios

**Goal:** Minimize message loss and ensure reliability

- **Buffer**:

  - Use `buffer_type: "disk"` with WAL for crash recovery
  - **Enable adaptive flushing:** `buffer_adaptive: true` (handles database slowdowns gracefully)
  - Set `buffer_wal_path` to persistent storage
  - Set `buffer_adaptive_max_size: 5000` (moderate ceiling for durability)
  - Set `buffer_adaptive_min_size: 100`
  - Use moderate `buffer_flush_interval_ms: 1000-3000`
  - Keep `num_buffer_nodes: 2-3` to avoid overwhelming database

- **Retries & Timeouts**:

  - Set `max_retries: 5-10` for multiple delivery attempts
  - Set `janitor_delete_failed_messages: false` to preserve failed messages in DLQ
  - Increase `msg_timeout_ms: 60000-300000` (1-5 minutes) based on processing time

- **Monitoring**:
  - Lower `health_check_interval_ms: 2000-3000` for faster failure detection
  - Enable DLQ monitoring to track failed messages
  - Monitor `buffer.adaptive_max_size` to detect database performance degradation

## Multi-Region Deployments (CockroachDB)

**Goal:** Global availability and low latency

- **Region Configuration**:

  - Set unique `region` value per deployment location (e.g., `us-east-1`, `eu-west-1`)
  - Use `datastore: "cockroach"` with proper `cockroach_region` setting
  - Enable `multi_region_supplement: true` for cross-region load balancing
  - Consider `multi_region_scheduler: true` and `multi_region_janitor: true` for global processing

- **Buffer Optimization**:

  - **Always enable adaptive flushing** in multi-region: `buffer_adaptive: true`
  - Adaptive flushing automatically handles variable cross-region latency
  - Set `buffer_adaptive_max_size: 5000-8000` (account for higher latency)
  - Monitor `avg_flush_duration` across regions to detect latency issues

- **Latency Considerations**:

  - Increase `msg_timeout_ms` to account for cross-region latency
  - Use higher `scheduler_poll_jitter_percent: 20-30` to prevent coordinated polls
  - Monitor database query latency and adjust `scheduler_poll_interval_ms` accordingly

- **Connection Pooling**:
  - Use higher connection limits: `cockroach_max_open_conns: 50-75`
  - Balance idle connections: `cockroach_max_idle_conns: 25-40`

## Adaptive Buffer Troubleshooting

**Buffer Size Keeps Decreasing:**

- **Cause:** Database is consistently slow (flushes taking >50% of interval)
- **Solution:** Investigate database performance, add indexes, or increase database resources
- **Temporary Fix:** Increase `buffer_flush_interval_ms` to give flushes more time

**Buffer Not Adapting Despite Load Changes:**

- **Cause:** `buffer_adaptive_tune_threshold` is too high
- **Solution:** Reduce `buffer_adaptive_tune_threshold` from 5 to 2-3 for more responsive tuning

**Frequent Size Oscillation:**

- **Cause:** `buffer_adaptive_tune_threshold` is too low, causing thrashing
- **Solution:** Increase `buffer_adaptive_tune_threshold` to 5-10 for more stable behavior

**Buffer Hitting Maximum Size:**

- **Cause:** Sustained high throughput with fast database
- **Action:** This is expected behavior! Increase `buffer_adaptive_max_size` if needed
