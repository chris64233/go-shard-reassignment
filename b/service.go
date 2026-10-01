package goshardreassignment

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// MigrationRequest describes a request to move a shard between nodes.
// RequestKey makes submission idempotent: resubmitting the same key
// returns the original task instead of creating a new one.
type MigrationRequest struct {
	RequestKey   string
	ShardID      string
	SourceNodeID string
	TargetNodeID string
	Reason       string
}

// Service registers shards, nodes and migration tasks while guaranteeing
// that a shard has exactly one effective owner at any moment.
// It is safe for concurrent use.
type Service struct {
	mu sync.RWMutex

	nodes      map[string]*Node
	shards     map[string]*Shard
	migrations map[string]*Migration
	byRequest  map[string]string // request key -> migration ID
	history    map[string][]HistoryEntry

	seq int
	now func() time.Time
}

// NewService creates an empty in-memory reassignment service.
func NewService() *Service {
	return &Service{
		nodes:      make(map[string]*Node),
		shards:     make(map[string]*Shard),
		migrations: make(map[string]*Migration),
		byRequest:  make(map[string]string),
		history:    make(map[string][]HistoryEntry),
		now:        time.Now,
	}
}

// RegisterNode adds a node in the active state.
func (s *Service) RegisterNode(id string) (Node, error) {
	if id == "" {
		return Node{}, fmt.Errorf("%w: id must not be empty", ErrNodeNotFound)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[id]; ok {
		return Node{}, fmt.Errorf("%w: %s", ErrNodeExists, id)
	}
	now := s.now()
	node := &Node{ID: id, Status: NodeStatusActive, CreatedAt: now, UpdatedAt: now}
	s.nodes[id] = node
	return *node, nil
}

// UpdateNodeStatus changes a node's status (active, draining or offline).
func (s *Service) UpdateNodeStatus(id string, status NodeStatus) error {
	switch status {
	case NodeStatusActive, NodeStatusDraining, NodeStatusOffline:
	default:
		return fmt.Errorf("%w: %q", ErrInvalidNodeStatus, status)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	node.Status = status
	node.UpdatedAt = s.now()
	return nil
}

// GetNode returns a node by ID.
func (s *Service) GetNode(id string) (Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	node, ok := s.nodes[id]
	if !ok {
		return Node{}, fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	return *node, nil
}

// ListNodes returns all nodes ordered by ID.
func (s *Service) ListNodes() []Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// CreateShard registers a shard with its capacity and initial owner node.
func (s *Service) CreateShard(id string, capacity int64, ownerNodeID string) (Shard, error) {
	if capacity <= 0 {
		return Shard{}, fmt.Errorf("%w: got %d", ErrInvalidShardCapacity, capacity)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.shards[id]; ok {
		return Shard{}, fmt.Errorf("%w: %s", ErrShardExists, id)
	}
	if _, ok := s.nodes[ownerNodeID]; !ok {
		return Shard{}, fmt.Errorf("%w: %s", ErrNodeNotFound, ownerNodeID)
	}
	now := s.now()
	shard := &Shard{ID: id, Capacity: capacity, OwnerNodeID: ownerNodeID, CreatedAt: now, UpdatedAt: now}
	s.shards[id] = shard
	s.appendHistoryLocked(HistoryEntry{
		ShardID:    id,
		Event:      HistoryEventShardCreated,
		ToNodeID:   ownerNodeID,
		Detail:     "shard registered",
		RecordedAt: now,
	})
	return *shard, nil
}

// GetShard returns the shard view with an explicit effective status.
// While a migration is active the owner remains the source node; the
// pending target is reported separately and never as the owner.
func (s *Service) GetShard(id string) (ShardView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	shard, ok := s.shards[id]
	if !ok {
		return ShardView{}, fmt.Errorf("%w: %s", ErrShardNotFound, id)
	}
	return s.shardViewLocked(shard), nil
}

// ListShards returns views of all shards ordered by ID.
func (s *Service) ListShards() []ShardView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ShardView, 0, len(s.shards))
	for _, shard := range s.shards {
		out = append(out, s.shardViewLocked(shard))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ShardID < out[j].ShardID })
	return out
}

func (s *Service) shardViewLocked(shard *Shard) ShardView {
	view := ShardView{
		ShardID:     shard.ID,
		Capacity:    shard.Capacity,
		OwnerNodeID: shard.OwnerNodeID,
	}
	owner, ok := s.nodes[shard.OwnerNodeID]
	if ok {
		view.OwnerNodeStatus = owner.Status
	}
	switch {
	case s.activeMigrationLocked(shard.ID) != nil:
		m := s.activeMigrationLocked(shard.ID)
		view.EffectiveStatus = ShardStatusMigrating
		view.ActiveMigrationID = m.ID
		view.PendingTargetNodeID = m.TargetNodeID
	case ok && owner.Status == NodeStatusOffline:
		view.EffectiveStatus = ShardStatusOwnerOffline
	default:
		view.EffectiveStatus = ShardStatusStable
	}
	return view
}

func (s *Service) activeMigrationLocked(shardID string) *Migration {
	for _, m := range s.migrations {
		if m.ShardID == shardID && m.State.Active() {
			return m
		}
	}
	return nil
}

// SubmitMigration creates a migration task in the pending state.
// The formal ownership does not change until the task completes.
// Resubmitting a request with the same RequestKey returns the original task.
func (s *Service) SubmitMigration(req MigrationRequest) (Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.RequestKey != "" {
		if id, ok := s.byRequest[req.RequestKey]; ok {
			return *s.migrations[id], nil
		}
	}

	shard, ok := s.shards[req.ShardID]
	if !ok {
		return Migration{}, fmt.Errorf("%w: %s", ErrShardNotFound, req.ShardID)
	}
	if _, ok := s.nodes[req.SourceNodeID]; !ok {
		return Migration{}, fmt.Errorf("%w: %s", ErrNodeNotFound, req.SourceNodeID)
	}
	target, ok := s.nodes[req.TargetNodeID]
	if !ok {
		return Migration{}, fmt.Errorf("%w: %s", ErrNodeNotFound, req.TargetNodeID)
	}
	if req.SourceNodeID == req.TargetNodeID {
		return Migration{}, ErrSameSourceTarget
	}
	if shard.OwnerNodeID != req.SourceNodeID {
		return Migration{}, fmt.Errorf("%w: shard %s owned by %s", ErrSourceNotOwner, req.ShardID, shard.OwnerNodeID)
	}
	if !target.Status.CanReceive() {
		return Migration{}, fmt.Errorf("%w: node %s is %s", ErrNodeCannotReceive, target.ID, target.Status)
	}
	if active := s.activeMigrationLocked(req.ShardID); active != nil {
		return Migration{}, fmt.Errorf("%w: shard %s held by migration %s", ErrShardMigrationActive, req.ShardID, active.ID)
	}

	now := s.now()
	s.seq++
	migration := &Migration{
		ID:           fmt.Sprintf("mig-%06d", s.seq),
		RequestKey:   req.RequestKey,
		ShardID:      req.ShardID,
		SourceNodeID: req.SourceNodeID,
		TargetNodeID: req.TargetNodeID,
		Reason:       req.Reason,
		State:        MigrationStatePending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	s.migrations[migration.ID] = migration
	if req.RequestKey != "" {
		s.byRequest[req.RequestKey] = migration.ID
	}
	return *migration, nil
}

// StartMigration moves a pending migration to in_progress.
func (s *Service) StartMigration(id string) (Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.migrations[id]
	if !ok {
		return Migration{}, fmt.Errorf("%w: %s", ErrMigrationNotFound, id)
	}
	if m.State != MigrationStatePending {
		return Migration{}, fmt.Errorf("%w: %s is %s", ErrInvalidMigrationState, id, m.State)
	}
	m.State = MigrationStateInProgress
	m.UpdatedAt = s.now()
	s.appendHistoryLocked(HistoryEntry{
		ShardID:     m.ShardID,
		MigrationID: m.ID,
		Event:       HistoryEventMigrationStarted,
		FromNodeID:  m.SourceNodeID,
		ToNodeID:    m.TargetNodeID,
		Detail:      m.Reason,
		RecordedAt:  m.UpdatedAt,
	})
	return *m, nil
}

// CompleteMigration finishes an in-progress migration and only now
// switches the shard's formal ownership to the target node.
func (s *Service) CompleteMigration(id string) (Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.migrations[id]
	if !ok {
		return Migration{}, fmt.Errorf("%w: %s", ErrMigrationNotFound, id)
	}
	if m.State != MigrationStateInProgress {
		return Migration{}, fmt.Errorf("%w: %s is %s", ErrInvalidMigrationState, id, m.State)
	}
	m.State = MigrationStateCompleted
	m.UpdatedAt = s.now()
	if shard, ok := s.shards[m.ShardID]; ok {
		shard.OwnerNodeID = m.TargetNodeID
		shard.UpdatedAt = m.UpdatedAt
	}
	s.appendHistoryLocked(HistoryEntry{
		ShardID:     m.ShardID,
		MigrationID: m.ID,
		Event:       HistoryEventMigrationComplete,
		FromNodeID:  m.SourceNodeID,
		ToNodeID:    m.TargetNodeID,
		Detail:      "ownership switched to target",
		RecordedAt:  m.UpdatedAt,
	})
	return *m, nil
}

// FailMigration marks a pending or in-progress migration as failed.
// Ownership stays with the source node and the failure reason is kept.
func (s *Service) FailMigration(id string, reason string) (Migration, error) {
	if reason == "" {
		return Migration{}, ErrEmptyFailureReason
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.migrations[id]
	if !ok {
		return Migration{}, fmt.Errorf("%w: %s", ErrMigrationNotFound, id)
	}
	if !m.State.Active() {
		return Migration{}, fmt.Errorf("%w: %s is %s", ErrInvalidMigrationState, id, m.State)
	}
	m.State = MigrationStateFailed
	m.FailureReason = reason
	m.UpdatedAt = s.now()
	s.appendHistoryLocked(HistoryEntry{
		ShardID:     m.ShardID,
		MigrationID: m.ID,
		Event:       HistoryEventMigrationFailed,
		FromNodeID:  m.SourceNodeID,
		ToNodeID:    m.TargetNodeID,
		Detail:      reason,
		RecordedAt:  m.UpdatedAt,
	})
	return *m, nil
}

// GetMigration returns a migration task by ID.
func (s *Service) GetMigration(id string) (Migration, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.migrations[id]
	if !ok {
		return Migration{}, fmt.Errorf("%w: %s", ErrMigrationNotFound, id)
	}
	return *m, nil
}

// ListMigrations returns migrations ordered by ID; shardID filters by shard
// and an empty shardID returns all migrations.
func (s *Service) ListMigrations(shardID string) []Migration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Migration, 0, len(s.migrations))
	for _, m := range s.migrations {
		if shardID == "" || m.ShardID == shardID {
			out = append(out, *m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ShardHistory returns the recorded events of a shard in chronological
// order, allowing ownership moves to be traced node by node.
func (s *Service) ShardHistory(shardID string) ([]HistoryEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.shards[shardID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrShardNotFound, shardID)
	}
	entries := s.history[shardID]
	out := make([]HistoryEntry, len(entries))
	copy(out, entries)
	return out, nil
}

func (s *Service) appendHistoryLocked(entry HistoryEntry) {
	s.history[entry.ShardID] = append(s.history[entry.ShardID], entry)
}
