package goshardreassignment

import "time"

// NodeStatus describes whether a node can receive shards.
type NodeStatus string

const (
	NodeStatusActive   NodeStatus = "active"
	NodeStatusDraining NodeStatus = "draining"
	NodeStatusOffline  NodeStatus = "offline"
)

// CanReceive reports whether the node may accept new shard assignments.
func (s NodeStatus) CanReceive() bool {
	return s == NodeStatusActive
}

// MigrationStatus is the lifecycle state of a migration task.
type MigrationStatus string

const (
	MigrationStatusPending    MigrationStatus = "pending"
	MigrationStatusInProgress MigrationStatus = "in_progress"
	MigrationStatusCompleted  MigrationStatus = "completed"
	MigrationStatusFailed     MigrationStatus = "failed"
)

// Active reports whether the migration still holds the shard's migration slot.
func (s MigrationStatus) Active() bool {
	return s == MigrationStatusPending || s == MigrationStatusInProgress
}

// ShardStatus is the effective serving state of a shard.
type ShardStatus string

const (
	ShardStatusActive    ShardStatus = "active"
	ShardStatusMigrating ShardStatus = "migrating"
	ShardStatusOffline   ShardStatus = "offline"
)

// Node is a storage node that can own shards.
type Node struct {
	ID        string
	Status    NodeStatus
	CreatedAt time.Time
}

// Shard is a data partition formally owned by exactly one node.
type Shard struct {
	ID        string
	Capacity  int64
	OwnerID   string
	CreatedAt time.Time
}

// Migration is a task moving a shard from a source node to a target node.
type Migration struct {
	ID           string
	ShardID      string
	SourceNodeID string
	TargetNodeID string
	Reason       string
	Status       MigrationStatus
	FailReason   string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// ShardView is the query result for a shard, exposing its effective status.
type ShardView struct {
	ShardID   string
	Capacity  int64
	OwnerID   string
	Status    ShardStatus
	Migration *Migration
}

// HistoryEventKind classifies assignment history entries.
type HistoryEventKind string

const (
	HistoryShardCreated       HistoryEventKind = "shard_created"
	HistoryMigrationSubmitted HistoryEventKind = "migration_submitted"
	HistoryMigrationStarted   HistoryEventKind = "migration_started"
	HistoryMigrationCompleted HistoryEventKind = "migration_completed"
	HistoryMigrationFailed    HistoryEventKind = "migration_failed"
	HistoryNodeStatusChanged  HistoryEventKind = "node_status_changed"
)

// HistoryEntry records one assignment-related change for audit queries.
type HistoryEntry struct {
	Seq        int64
	Kind       HistoryEventKind
	ShardID    string
	FromNodeID string
	ToNodeID   string
	Detail     string
	At         time.Time
}
