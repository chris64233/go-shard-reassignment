package goshardreassignment

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	ErrNodeNotFound      = errors.New("node not found")
	ErrNodeExists        = errors.New("node already exists")
	ErrShardNotFound     = errors.New("shard not found")
	ErrShardExists       = errors.New("shard already exists")
	ErrMigrationNotFound = errors.New("migration not found")
	ErrInvalidCapacity   = errors.New("shard capacity must be positive")
	ErrEmptyID           = errors.New("id must not be empty")
)

// Service registers shards, nodes and migration tasks while guaranteeing
// that a shard has exactly one effective owner at any time.
type Service struct {
	mu         sync.RWMutex
	nodes      map[string]*Node
	shards     map[string]*Shard
	migrations map[string]*Migration
	// activeByShard maps a shard ID to its active (pending/in-progress)
	// migration ID, enforcing at most one active migration per shard.
	activeByShard map[string]string
	history       []HistoryEntry
	seq           int64
	migSeq        int64
	now           func() time.Time
}

// NewService creates an empty shard reassignment service.
func NewService() *Service {
	return &Service{
		nodes:         make(map[string]*Node),
		shards:        make(map[string]*Shard),
		migrations:    make(map[string]*Migration),
		activeByShard: make(map[string]string),
		now:           time.Now,
	}
}

// CreateNode registers a node. Duplicate node IDs are rejected.
func (s *Service) CreateNode(id string, status NodeStatus) (*Node, error) {
	if id == "" {
		return nil, ErrEmptyID
	}
	switch status {
	case NodeStatusActive, NodeStatusDraining, NodeStatusOffline:
	default:
		return nil, fmt.Errorf("unknown node status %q", status)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[id]; ok {
		return nil, fmt.Errorf("%w: %s", ErrNodeExists, id)
	}
	node := &Node{ID: id, Status: status, CreatedAt: s.now()}
	s.nodes[id] = node
	s.appendHistoryLocked(HistoryEntry{
		Kind:     HistoryNodeStatusChanged,
		ToNodeID: id,
		Detail:   fmt.Sprintf("node registered with status %s", status),
	})
	copied := *node
	return &copied, nil
}

// SetNodeStatus updates a node's status, e.g. marking it offline.
func (s *Service) SetNodeStatus(id string, status NodeStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	switch status {
	case NodeStatusActive, NodeStatusDraining, NodeStatusOffline:
	default:
		return fmt.Errorf("unknown node status %q", status)
	}
	if node.Status == status {
		return nil
	}
	node.Status = status
	s.appendHistoryLocked(HistoryEntry{
		Kind:     HistoryNodeStatusChanged,
		ToNodeID: id,
		Detail:   fmt.Sprintf("node status changed to %s", status),
	})
	return nil
}

// CreateShard registers a shard owned by an existing node. Duplicate shard
// IDs, unknown owner nodes and non-positive capacities are rejected.
func (s *Service) CreateShard(id string, capacity int64, ownerNodeID string) (*Shard, error) {
	if id == "" {
		return nil, ErrEmptyID
	}
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.shards[id]; ok {
		return nil, fmt.Errorf("%w: %s", ErrShardExists, id)
	}
	if _, ok := s.nodes[ownerNodeID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrNodeNotFound, ownerNodeID)
	}
	shard := &Shard{ID: id, Capacity: capacity, OwnerID: ownerNodeID, CreatedAt: s.now()}
	s.shards[id] = shard
	s.appendHistoryLocked(HistoryEntry{
		Kind:     HistoryShardCreated,
		ShardID:  id,
		ToNodeID: ownerNodeID,
		Detail:   "shard created",
	})
	copied := *shard
	return &copied, nil
}

// SubmitMigration creates a migration task for a shard towards a target
// node. Submitting the same (shard, target, reason) request while an
// identical active migration exists returns the original task. A different
// active migration on the same shard is rejected.
func (s *Service) SubmitMigration(shardID, targetNodeID, reason string) (*Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	shard, ok := s.shards[shardID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrShardNotFound, shardID)
	}
	target, ok := s.nodes[targetNodeID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNodeNotFound, targetNodeID)
	}
	if !target.Status.CanReceive() {
		return nil, fmt.Errorf("target node %s is %s and cannot receive shards", targetNodeID, target.Status)
	}
	if shard.OwnerID == targetNodeID {
		return nil, fmt.Errorf("shard %s is already owned by node %s", shardID, targetNodeID)
	}

	if activeID, busy := s.activeByShard[shardID]; busy {
		existing := s.migrations[activeID]
		if existing.TargetNodeID == targetNodeID && existing.Reason == reason {
			copied := *existing
			return &copied, nil
		}
		return nil, fmt.Errorf("shard %s already has an active migration %s", shardID, activeID)
	}

	s.migSeq++
	migration := &Migration{
		ID:           fmt.Sprintf("mig-%d", s.migSeq),
		ShardID:      shardID,
		SourceNodeID: shard.OwnerID,
		TargetNodeID: targetNodeID,
		Reason:       reason,
		Status:       MigrationStatusPending,
		CreatedAt:    s.now(),
	}
	migration.UpdatedAt = migration.CreatedAt
	s.migrations[migration.ID] = migration
	s.activeByShard[shardID] = migration.ID
	s.appendHistoryLocked(HistoryEntry{
		Kind:       HistoryMigrationSubmitted,
		ShardID:    shardID,
		FromNodeID: migration.SourceNodeID,
		ToNodeID:   targetNodeID,
		Detail:     fmt.Sprintf("migration %s submitted: %s", migration.ID, reason),
	})
	copied := *migration
	return &copied, nil
}

// StartMigration transitions a pending migration to in-progress.
func (s *Service) StartMigration(migrationID string) (*Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	migration, err := s.migrationLocked(migrationID)
	if err != nil {
		return nil, err
	}
	if migration.Status != MigrationStatusPending {
		return nil, fmt.Errorf("migration %s is %s, cannot start", migrationID, migration.Status)
	}
	migration.Status = MigrationStatusInProgress
	migration.UpdatedAt = s.now()
	s.appendHistoryLocked(HistoryEntry{
		Kind:       HistoryMigrationStarted,
		ShardID:    migration.ShardID,
		FromNodeID: migration.SourceNodeID,
		ToNodeID:   migration.TargetNodeID,
		Detail:     fmt.Sprintf("migration %s started", migration.ID),
	})
	copied := *migration
	return &copied, nil
}

// CompleteMigration finishes an in-progress migration and moves the shard's
// formal ownership to the target node. Ownership is untouched before this
// point.
func (s *Service) CompleteMigration(migrationID string) (*Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	migration, err := s.migrationLocked(migrationID)
	if err != nil {
		return nil, err
	}
	if migration.Status != MigrationStatusInProgress {
		return nil, fmt.Errorf("migration %s is %s, cannot complete", migrationID, migration.Status)
	}
	migration.Status = MigrationStatusCompleted
	migration.UpdatedAt = s.now()
	delete(s.activeByShard, migration.ShardID)

	shard := s.shards[migration.ShardID]
	shard.OwnerID = migration.TargetNodeID
	s.appendHistoryLocked(HistoryEntry{
		Kind:       HistoryMigrationCompleted,
		ShardID:    migration.ShardID,
		FromNodeID: migration.SourceNodeID,
		ToNodeID:   migration.TargetNodeID,
		Detail:     fmt.Sprintf("migration %s completed, ownership moved", migration.ID),
	})
	copied := *migration
	return &copied, nil
}

// FailMigration marks a pending or in-progress migration as failed and
// records the failure reason. Formal ownership stays with the source node,
// so the shard can be migrated again afterwards.
func (s *Service) FailMigration(migrationID, failReason string) (*Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	migration, err := s.migrationLocked(migrationID)
	if err != nil {
		return nil, err
	}
	if !migration.Status.Active() {
		return nil, fmt.Errorf("migration %s is %s, cannot fail", migrationID, migration.Status)
	}
	migration.Status = MigrationStatusFailed
	migration.FailReason = failReason
	migration.UpdatedAt = s.now()
	delete(s.activeByShard, migration.ShardID)
	s.appendHistoryLocked(HistoryEntry{
		Kind:       HistoryMigrationFailed,
		ShardID:    migration.ShardID,
		FromNodeID: migration.SourceNodeID,
		ToNodeID:   migration.TargetNodeID,
		Detail:     fmt.Sprintf("migration %s failed: %s", migration.ID, failReason),
	})
	copied := *migration
	return &copied, nil
}

// GetShard returns the shard's effective view. The owner is always the
// formal owner; a temporary migration target is never reported as owner.
// Status is "migrating" while an active migration exists, and "offline"
// when the owning node is offline.
func (s *Service) GetShard(shardID string) (*ShardView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	shard, ok := s.shards[shardID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrShardNotFound, shardID)
	}
	view := &ShardView{
		ShardID:  shard.ID,
		Capacity: shard.Capacity,
		OwnerID:  shard.OwnerID,
		Status:   ShardStatusActive,
	}
	if activeID, busy := s.activeByShard[shardID]; busy {
		view.Status = ShardStatusMigrating
		migration := *s.migrations[activeID]
		view.Migration = &migration
	}
	if node, ok := s.nodes[shard.OwnerID]; ok && node.Status == NodeStatusOffline {
		view.Status = ShardStatusOffline
	}
	return view, nil
}

// GetNode returns a node by ID.
func (s *Service) GetNode(nodeID string) (*Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	node, ok := s.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNodeNotFound, nodeID)
	}
	copied := *node
	return &copied, nil
}

// GetMigration returns a migration task by ID.
func (s *Service) GetMigration(migrationID string) (*Migration, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	migration, err := s.migrationLocked(migrationID)
	if err != nil {
		return nil, err
	}
	copied := *migration
	return &copied, nil
}

// ListShards returns all shard views sorted by shard ID.
func (s *Service) ListShards() ([]ShardView, error) {
	s.mu.RLock()
	ids := make([]string, 0, len(s.shards))
	for id := range s.shards {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	sort.Strings(ids)
	views := make([]ShardView, 0, len(ids))
	for _, id := range ids {
		view, err := s.GetShard(id)
		if err != nil {
			return nil, err
		}
		views = append(views, *view)
	}
	return views, nil
}

// ListNodes returns all nodes sorted by node ID.
func (s *Service) ListNodes() []Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	nodes := make([]Node, 0, len(s.nodes))
	for _, node := range s.nodes {
		nodes = append(nodes, *node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}

// ListMigrations returns all migration tasks sorted by ID.
func (s *Service) ListMigrations() []Migration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	migrations := make([]Migration, 0, len(s.migrations))
	for _, migration := range s.migrations {
		migrations = append(migrations, *migration)
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].ID < migrations[j].ID })
	return migrations
}

// ShardHistory returns the assignment history entries for one shard in
// chronological order, tracing how it moved between nodes.
func (s *Service) ShardHistory(shardID string) []HistoryEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var entries []HistoryEntry
	for _, entry := range s.history {
		if entry.ShardID == shardID {
			entries = append(entries, entry)
		}
	}
	return entries
}

// History returns the full assignment history in chronological order.
func (s *Service) History() []HistoryEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := make([]HistoryEntry, len(s.history))
	copy(entries, s.history)
	return entries
}

func (s *Service) migrationLocked(migrationID string) (*Migration, error) {
	migration, ok := s.migrations[migrationID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMigrationNotFound, migrationID)
	}
	return migration, nil
}

func (s *Service) appendHistoryLocked(entry HistoryEntry) {
	s.seq++
	entry.Seq = s.seq
	entry.At = s.now()
	s.history = append(s.history, entry)
}
