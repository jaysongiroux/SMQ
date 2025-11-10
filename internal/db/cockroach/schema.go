package cockroach

import "github.com/jaysongiroux/smq/internal/logger"

// Schema contains the SQL statements to set up the CockroachDB database schema
// for a multi-region deployment.
const Schema = `
-- ---
-- Database Setup (Run this manually before executing the schema)
-- ---

-- Set the database context for the session.
USE smq;

-- Messages table
-- Stores all scheduled messages with REGIONAL BY ROW locality for automatic
-- region-based partitioning and fast local-first queries.
CREATE TABLE IF NOT EXISTS messages (
    -- Use gen_random_uuid() as the default for the PK.
    -- This prevents write hot spots, which is critical for CRDB performance.
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    channel STRING NOT NULL,
    payload JSONB NOT NULL,
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    status STRING NOT NULL CHECK (status IN ('PENDING', 'READY', 'ACQUIRED', 'FAILED')),
    acquired_at TIMESTAMP WITH TIME ZONE,
    retry_count INT NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- CockroachDB's special region column that determines row locality.
    -- This column uses the crdb_internal_region enum type (automatically created
    -- when you set up multi-region). It defaults to the region where the SQL
    -- gateway handling the request is located.
    crdb_region crdb_internal_region NOT NULL DEFAULT gateway_region()::crdb_internal_region
    
) LOCALITY REGIONAL BY ROW AS crdb_region;

-- Indexes for messages table
-- CockroachDB automatically optimizes these for regional queries.

-- Scheduler index: efficiently find PENDING messages in the local region.
CREATE INDEX IF NOT EXISTS idx_messages_scheduler ON messages(status, scheduled_at) 
    WHERE status = 'PENDING';

-- Consumer index: efficiently find READY messages for polling in the local region.
CREATE INDEX IF NOT EXISTS idx_messages_consumer ON messages(channel, status, scheduled_at) 
    WHERE status = 'READY';

-- Janitor index: efficiently find stale ACQUIRED messages in the local region.
CREATE INDEX IF NOT EXISTS idx_messages_janitor ON messages(status, acquired_at)
    WHERE status = 'ACQUIRED' AND acquired_at IS NOT NULL;

-- Nodes table
-- Stores cluster node information for health tracking.
-- This is a 'GLOBAL' table: it is replicated to all regions for fast reads
-- from any node. Ideal for small, frequently-read metadata.
CREATE TABLE IF NOT EXISTS nodes (
    node_id STRING PRIMARY KEY,
    status STRING NOT NULL DEFAULT 'pending',
    last_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    registered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    metadata JSONB
) LOCALITY GLOBAL;

-- Index for querying nodes by status (fast reads from any region)
CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);

-- Index for finding stale nodes (fast reads from any region)
CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen);
`

func InformUserAboutPartitions(log *logger.Logger) {
	log.Info("--------------------------------")
	log.Info("You must manually create partitions for each region and attach them to the corresponding tablespace.")
	log.Info("Example:")
	log.Info("CREATE TABLE messages_us_east_1 PARTITION OF messages FOR VALUES IN ('us-east-1') TABLESPACE us_east_1_ts;")
	log.Info("CREATE TABLE messages_us_west_1 PARTITION OF messages FOR VALUES IN ('us-west-1') TABLESPACE us_west_1_ts;")
	log.Info("--------------------------------")
}
