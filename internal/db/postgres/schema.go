package postgres

// Schema contains the SQL statements to set up the PostgreSQL database schema
const Schema = `
-- Messages table
-- Stores all scheduled messages in the queue
CREATE TABLE IF NOT EXISTS messages (
    id UUID PRIMARY KEY,
    channel VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('PENDING', 'READY', 'ACQUIRED', 'FAILED')),
    acquired_at TIMESTAMP WITH TIME ZONE,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    region VARCHAR(100)
);

-- Indexes for messages table
-- Scheduler index: efficiently find PENDING messages that are ready
CREATE INDEX IF NOT EXISTS idx_messages_scheduler ON messages(status, scheduled_at) 
    WHERE status = 'PENDING';

-- Consumer index: efficiently find READY messages for polling by channel
CREATE INDEX IF NOT EXISTS idx_messages_consumer ON messages(channel, status, scheduled_at) 
    WHERE status = 'READY';

-- Janitor index: efficiently find stale ACQUIRED messages
CREATE INDEX IF NOT EXISTS idx_messages_janitor ON messages(status, acquired_at)
    WHERE status = 'ACQUIRED' AND acquired_at IS NOT NULL;

-- Nodes table
-- Stores cluster node information for health tracking
CREATE TABLE IF NOT EXISTS nodes (
    node_id VARCHAR(255) PRIMARY KEY,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    last_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    registered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    metadata JSONB
);

-- Index for querying nodes by status
CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);

-- Index for finding stale nodes
CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen);
`
