package goshardreassignment

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Event kind constants. Every state transition is represented by exactly
// one event; the service durably appends the event before observing it in
// memory, so a crash never loses acknowledged state and a restart replays
// identically.
const (
	KindShardRegistered = "shard_registered"
	KindHealthReported  = "health_reported"
	KindConfigChanged   = "config_changed"
	KindPlanCreated     = "plan_created"
	KindStepEntered     = "step_entered"
	KindReceiptRecorded = "receipt_recorded"
	KindStepAdvanced    = "step_advanced"
	KindPlanPaused      = "plan_paused"
	KindPlanResumed     = "plan_resumed"
	KindPlanCancelled   = "plan_cancelled"
	KindPlanCompleted   = "plan_completed"
)

// Reasons recorded on config_changed events.
const (
	ChangeReasonManual   = "manual"
	ChangeReasonAdd      = "migration_add"
	ChangeReasonSwitch   = "migration_switch"
	ChangeReasonRemove   = "migration_remove"
	ChangeReasonRollback = "cancel_rollback"
)

// ShardRegisteredDetail is the payload of the version-1 registration.
type ShardRegisteredDetail struct {
	Primary    ReplicaID   `json:"primary"`
	Members    []ReplicaID `json:"members"`
	MinHealthy int         `json:"min_healthy"`
}

// HealthReportedDetail replaces the healthy set; versions never change.
type HealthReportedDetail struct {
	Healthy []ReplicaID `json:"healthy"`
}

// ConfigChangeDetail is the payload of every version increment after
// registration (manual edits, migration add/switch/remove, cancel rollback).
type ConfigChangeDetail struct {
	Reason     string      `json:"reason"`
	OldVersion int64       `json:"old_version"`
	NewVersion int64       `json:"new_version"`
	OldPrimary ReplicaID   `json:"old_primary,omitempty"`
	NewPrimary ReplicaID   `json:"new_primary"`
	OldMembers []ReplicaID `json:"old_members,omitempty"`
	NewMembers []ReplicaID `json:"new_members"`
	MinHealthy int         `json:"min_healthy"`
	Note       string      `json:"note,omitempty"`
}

// PlanCreatedDetail freezes the target and the derived steps.
type PlanCreatedDetail struct {
	PlanID        string      `json:"plan_id"`
	StartVersion  int64       `json:"start_version"`
	TargetPrimary ReplicaID   `json:"target_primary"`
	TargetMembers []ReplicaID `json:"target_members"`
	Steps         []Step      `json:"steps"`
	CreatedAt     string      `json:"created_at"`
}

// StepEnteredDetail marks a step becoming in-flight and pins the config
// version its receipts must carry.
type StepEnteredDetail struct {
	PlanID        string `json:"plan_id"`
	Step          int    `json:"step"`
	Kind          string `json:"kind"`
	ConfigVersion int64  `json:"config_version"`
	At            string `json:"at"`
}

// ReceiptRecordedDetail is one accepted node receipt. The Advanced and
// WaitingHealthy flags capture the first-time outcome so that replaying
// the stream reproduces the exact ReceiptOutcome; a duplicate receipt
// returns these recorded flags instead of re-evaluating state.
type ReceiptRecordedDetail struct {
	PlanID         string    `json:"plan_id"`
	Step           int       `json:"step"`
	ConfigVersion  int64     `json:"config_version"`
	Node           ReplicaID `json:"node"`
	OK             bool      `json:"ok"`
	Message        string    `json:"message,omitempty"`
	Advanced       bool      `json:"advanced"`
	WaitingHealthy bool      `json:"waiting_healthy"`
	At             string    `json:"at"`
}

// StepAdvancedDetail marks safe completion of a step.
type StepAdvancedDetail struct {
	PlanID string `json:"plan_id"`
	Step   int    `json:"step"`
	At     string `json:"at"`
}

// PlanPausedDetail records why a plan stopped.
type PlanPausedDetail struct {
	PlanID string      `json:"plan_id"`
	Reason PauseReason `json:"reason"`
	Detail string      `json:"detail,omitempty"`
	At     string      `json:"at"`
}

// PlanResumedDetail marks continuation of a paused plan.
type PlanResumedDetail struct {
	PlanID string `json:"plan_id"`
	At     string `json:"at"`
}

// PlanCancelledDetail records the configuration left in place after cancel.
type PlanCancelledDetail struct {
	PlanID string `json:"plan_id"`
	// EffectiveConfig is the resulting configuration. RolledBack is true
	// when not-yet-effective target changes were undone (pre-switch cancel);
	// false when the migration tail was finalized after a completed switch
	// or when a conflict-paused plan was cancelled without touching the
	// newer configuration.
	EffectiveConfig ShardConfig `json:"effective_config"`
	RolledBack      bool        `json:"rolled_back"`
	At              string      `json:"at"`
}

// PlanCompletedDetail marks successful migration to the target.
type PlanCompletedDetail struct {
	PlanID string `json:"plan_id"`
	At     string `json:"at"`
}

// Event is one durable record. Detail is one of the *Detail types.
type Event struct {
	Seq     int64
	Kind    string
	ShardID ShardID
	PlanID  string
	Version int64
	At      string
	Detail  any
}

// Store persists and replays the append-only event stream. Implementations
// must be safe for concurrent use.
type Store interface {
	// Append durably records an event that already carries its final seq.
	Append(event Event) error
	// Replay calls fn for every stored event in sequence order.
	Replay(fn func(Event) error) error
	// Close releases underlying resources.
	Close() error
}

// decodeDetail unmarshals the raw JSON detail into the typed payload
// registered for the event kind.
func decodeDetail(kind string, raw json.RawMessage) (any, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var target any
	switch kind {
	case KindShardRegistered:
		target = &ShardRegisteredDetail{}
	case KindHealthReported:
	target = &HealthReportedDetail{}
	case KindConfigChanged:
	target = &ConfigChangeDetail{}
	case KindPlanCreated:
	target = &PlanCreatedDetail{}
	case KindStepEntered:
	target = &StepEnteredDetail{}
	case KindReceiptRecorded:
	target = &ReceiptRecordedDetail{}
	case KindStepAdvanced:
	target = &StepAdvancedDetail{}
	case KindPlanPaused:
	target = &PlanPausedDetail{}
	case KindPlanResumed:
	target = &PlanResumedDetail{}
	case KindPlanCancelled:
	target = &PlanCancelledDetail{}
	case KindPlanCompleted:
	target = &PlanCompletedDetail{}
	default:
		return nil, fmt.Errorf("unknown event kind %q", kind)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return nil, err
	}
	return target, nil
}

// MemoryStore keeps the event stream in memory; it is useful for tests and
// ephemeral deployments.
type MemoryStore struct {
	mu     sync.Mutex
	events []Event
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

// Append records the event in memory.
func (s *MemoryStore) Append(event Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

// Replay replays every stored event in order.
func (s *MemoryStore) Replay(fn func(Event) error) error {
	s.mu.Lock()
	events := make([]Event, len(s.events))
	copy(events, s.events)
	s.mu.Unlock()
	for _, event := range events {
		if err := fn(event); err != nil {
			return err
		}
	}
	return nil
}

// Close is a no-op for the memory store.
func (s *MemoryStore) Close() error { return nil }
