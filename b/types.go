package goshardreassignment

import "time"

// NodeStatus describes whether a node can receive shards.
type NodeStatus string

const (
	// NodeStatusActive means the node is healthy and can receive shards.
	NodeStatusActive NodeStatus = "active"
	// NodeStatusDraining means the node is being emptied and cannot receive new shards.
	NodeStatusDraining NodeStatus = "draining"
	// NodeStatusOffline means the node is unreachable and cannot receive shards.
	NodeStatusOffline NodeStatus = "offline"
)

// CanReceive reports whether a node in this status may be a migration target.
func (s NodeStatus) CanReceive() bool {
	return s == NodeStatusActive
}

// Node is a storage node that can own shards.
type Node struct {
	ID        string
	Status    NodeStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ShardEffectiveStatus is the externally visible state of a shard.
type ShardEffectiveStatus string

const (
	// ShardStatusStable means the shard has a healthy owner and no active migration.
	ShardStatusStable ShardEffectiveStatus = "stable"
	// ShardStatusMigrating means an active migration exists; the owner is still the source node.
	ShardStatusMigrating ShardEffectiveStatus = "migrating"
	// ShardStatusOwnerOffline means the current owner node is offline.
	ShardStatusOwnerOffline ShardEffectiveStatus = "owner_offline"
)

// Shard is a data partition owned by exactly one node at a time.
type Shard struct {
	ID          string
	Capacity    int64
	OwnerNodeID string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ShardView is the query result for a shard: it never exposes a pending
// migration target as the owner and always carries an explicit status.
type ShardView struct {
	ShardID             string
	Capacity            int64
	OwnerNodeID         string
	OwnerNodeStatus     NodeStatus
	EffectiveStatus     ShardEffectiveStatus
	ActiveMigrationID   string
	PendingTargetNodeID string
}

// MigrationState is the lifecycle state of a migration task.
type MigrationState string

const (
	MigrationStatePending    MigrationState = "pending"
	MigrationStateInProgress MigrationState = "in_progress"
	MigrationStateCompleted  MigrationState = "completed"
	MigrationStateFailed     MigrationState = "failed"
)

// Active reports whether the migration still occupies the shard.
func (s MigrationState) Active() bool {
	return s == MigrationStatePending || s == MigrationStateInProgress
}

// Migration is a task moving one shard from a source node to a target node.
type Migration struct {
	ID            string
	RequestKey    string
	ShardID       string
	SourceNodeID  string
	TargetNodeID  string
	Reason        string
	State         MigrationState
	FailureReason string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// HistoryEventType classifies ownership-affecting events.
type HistoryEventType string

const (
	HistoryEventShardCreated      HistoryEventType = "shard_created"
	HistoryEventMigrationStarted  HistoryEventType = "migration_started"
	HistoryEventMigrationComplete HistoryEventType = "migration_completed"
	HistoryEventMigrationFailed   HistoryEventType = "migration_failed"
)

// HistoryEntry records one step of a shard moving between nodes.
type HistoryEntry struct {
	ShardID     string
	MigrationID string
	Event       HistoryEventType
	FromNodeID  string
	ToNodeID    string
	Detail      string
	RecordedAt  time.Time
}
