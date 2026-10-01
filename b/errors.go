package goshardreassignment

import "errors"

var (
	// ErrNodeExists is returned when registering a node ID that already exists.
	ErrNodeExists = errors.New("node already exists")
	// ErrNodeNotFound is returned when referencing an unknown node.
	ErrNodeNotFound = errors.New("node not found")
	// ErrInvalidNodeStatus is returned for an unknown node status value.
	ErrInvalidNodeStatus = errors.New("invalid node status")

	// ErrShardExists is returned when creating a shard ID that already exists.
	ErrShardExists = errors.New("shard already exists")
	// ErrShardNotFound is returned when referencing an unknown shard.
	ErrShardNotFound = errors.New("shard not found")
	// ErrInvalidShardCapacity is returned for non-positive shard capacity.
	ErrInvalidShardCapacity = errors.New("shard capacity must be positive")

	// ErrNodeCannotReceive is returned when the migration target cannot receive shards.
	ErrNodeCannotReceive = errors.New("target node cannot receive shards")
	// ErrSourceNotOwner is returned when the source node does not own the shard.
	ErrSourceNotOwner = errors.New("source node is not the shard owner")
	// ErrSameSourceTarget is returned when source and target nodes are identical.
	ErrSameSourceTarget = errors.New("source and target nodes must differ")
	// ErrShardMigrationActive is returned when the shard already has an active migration.
	ErrShardMigrationActive = errors.New("shard already has an active migration")

	// ErrMigrationNotFound is returned when referencing an unknown migration.
	ErrMigrationNotFound = errors.New("migration not found")
	// ErrInvalidMigrationState is returned for an illegal state transition.
	ErrInvalidMigrationState = errors.New("invalid migration state transition")
	// ErrEmptyFailureReason is returned when failing a migration without a reason.
	ErrEmptyFailureReason = errors.New("failure reason must not be empty")
)
