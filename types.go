package goshardreassignment

// ReplicaID identifies a replica node. It is opaque to the service.
type ReplicaID string

// ShardID identifies a shard within the service.
type ShardID string

// StepKind enumerates the mandatory, ordered migration phases.
//
// A safe migration never removes an old replica before the replacement has
// joined, synced and taken over its role:
//
//	StepAdd -> StepSync -> StepSwitch -> StepRemove
type StepKind string

const (
	// StepAdd brings the genuinely new target replicas into the replica
	// set. The join takes effect when the step is entered: a new config
	// version is persisted before any receipt is accepted.
	StepAdd StepKind = "add"
	// StepSync waits until every newly added replica is healthy and has
	// caught up with the current primary.
	StepSync StepKind = "sync"
	// StepSwitch promotes the target replica to primary. The promotion is
	// committed (a new config version) only after every required receipt
	// arrived and the target primary is healthy.
	StepSwitch StepKind = "switch"
	// StepRemove removes old replicas last, only after the switch has been
	// safely recorded and the remaining set still satisfies MinHealthy.
	StepRemove StepKind = "remove"
)

// PlanStatus is the lifecycle state of a migration plan.
type PlanStatus string

const (
	// PlanActive means the plan drives the migration forward.
	PlanActive PlanStatus = "active"
	// PlanPaused means the plan is stopped and must be resumed or cancelled
	// before it can progress.
	PlanPaused PlanStatus = "paused"
	// PlanCompleted means every step finished and the target configuration
	// is effective. Terminal state.
	PlanCompleted PlanStatus = "completed"
	// PlanCancelled means the plan was cancelled. Depending on when
	// cancellation happened the effective configuration was either rolled
	// back to the frozen start or finalized safely past the switch.
	PlanCancelled PlanStatus = "cancelled"
)

// PauseReason explains why a plan is paused.
type PauseReason string

const (
	// PauseManual: an operator called Pause.
	PauseManual PauseReason = "manual"
	// PauseFailure: a node reported a failing receipt for the in-flight
	// step. Already completed steps remain in place.
	PauseFailure PauseReason = "failure"
	// PauseConflict: the frozen starting version was invalidated by a
	// manual configuration change. The plan can only be cancelled and must
	// never overwrite the newer configuration.
	PauseConflict PauseReason = "conflict"
)

// IsTerminal reports whether the status is completed or cancelled.
func (s PlanStatus) IsTerminal() bool { return s == PlanCompleted || s == PlanCancelled }

// ShardConfig is an immutable snapshot of a shard configuration.
type ShardConfig struct {
	// Version is monotonically increasing starting at 1. Every effective
	// configuration change increments it and is persisted.
	Version int64 `json:"version"`
	// Primary is the current primary replica.
	Primary ReplicaID `json:"primary"`
	// Members is the full ordered set of configured replicas.
	Members []ReplicaID `json:"members"`
	// MinHealthy is the minimum number of healthy replicas required at all
	// times. Migration steps never decrease it.
	MinHealthy int `json:"min_healthy"`
}

// Clone returns a deep copy so callers cannot mutate service state through
// a returned snapshot.
func (c ShardConfig) Clone() ShardConfig {
	members := make([]ReplicaID, len(c.Members))
	copy(members, c.Members)
	return ShardConfig{Version: c.Version, Primary: c.Primary, Members: members, MinHealthy: c.MinHealthy}
}

// Step describes one migration phase.
type Step struct {
	// Index is the zero-based position within the plan.
	Index int `json:"index"`
	// Kind is the phase kind.
	Kind StepKind `json:"kind"`
	// Nodes are the replicas whose receipts are required to advance.
	Nodes []ReplicaID `json:"nodes"`
	// ConfigVersion is the version receipts of this step must echo. It is
	// assigned when the step is entered (zero before then):
	//   - add:    the version after the join took effect
	//   - sync:   the version after the join
	//   - switch: the version in effect while the switch is acknowledged;
	//             the promotion commits only after all receipts arrived
	//   - remove: the version in effect while removal is acknowledged;
	//             removal commits only after all receipts and a safety check
	ConfigVersion int64 `json:"config_version"`
	// Done marks steps that already completed safely.
	Done bool `json:"done"`
}

// Clone returns a deep copy of the step.
func (s Step) Clone() Step {
	nodes := make([]ReplicaID, len(s.Nodes))
	copy(nodes, s.Nodes)
	return Step{Index: s.Index, Kind: s.Kind, Nodes: nodes, ConfigVersion: s.ConfigVersion, Done: s.Done}
}

// Plan is an immutable snapshot of a migration plan.
type Plan struct {
	// ID is the service-wide unique plan identifier.
	ID string `json:"id"`
	// ShardID is the shard being migrated.
	ShardID ShardID `json:"shard_id"`
	// StartConfig is the configuration frozen when the plan was created.
	StartConfig ShardConfig `json:"start_config"`
	// TargetPrimary is the desired primary after migration.
	TargetPrimary ReplicaID `json:"target_primary"`
	// TargetMembers is the desired ordered replica set after migration.
	TargetMembers []ReplicaID `json:"target_members"`
	// Steps are the ordered mandatory phases (empty phases are omitted).
	Steps []Step `json:"steps"`
	// CurrentStep is the in-flight step index, or len(Steps) when done.
	CurrentStep int `json:"current_step"`
	// Status is the current lifecycle status.
	Status PlanStatus `json:"status"`
	// PauseReason is set while Status is PlanPaused.
	PauseReason PauseReason `json:"pause_reason,omitempty"`
	// FailureDetail carries the pause description (failure/conflict).
	FailureDetail string `json:"failure_detail,omitempty"`
	// CreatedAt is the creation timestamp (RFC3339, UTC).
	CreatedAt string `json:"created_at"`
	// UpdatedAt is the timestamp of the last status change (RFC3339, UTC).
	UpdatedAt string `json:"updated_at"`
}

// Clone returns a deep copy of the plan snapshot.
func (p Plan) Clone() Plan {
	out := p
	out.StartConfig = p.StartConfig.Clone()
	out.TargetMembers = cloneIDs(p.TargetMembers)
	out.Steps = make([]Step, len(p.Steps))
	for i, step := range p.Steps {
	out.Steps[i] = step.Clone()
	}
	return out
}

// Receipt is a node acknowledgement for a plan step. Every receipt must
// carry the plan id, the current step index and the step's configuration
// version; receipts missing any of these are stale or invalid and never
// advance the plan.
type Receipt struct {
	PlanID        string    `json:"plan_id"`
	Step          int       `json:"step"`
	ConfigVersion int64     `json:"config_version"`
	Node          ReplicaID `json:"node"`
	// OK is true for a positive acknowledgement, false for a failure report
	// (which pauses the plan with PauseFailure).
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// ReceiptOutcome is returned from SubmitReceipt. Re-submitting an identical
// already-recorded receipt returns Duplicate=true and the first-time
// outcome, and never changes state.
type ReceiptOutcome struct {
	// Duplicate reports whether the identical receipt was already recorded.
	Duplicate bool `json:"duplicate"`
	// Accepted reports whether the receipt was valid for the current step.
	Accepted bool `json:"accepted"`
	// Advanced reports whether the plan moved (to a later step or to
	// completion) while processing this receipt.
	Advanced bool `json:"advanced"`
	// WaitingHealthy reports that receipts are complete but the plan is
	// blocked on replica health (or would violate the safety line).
	WaitingHealthy bool `json:"waiting_healthy"`
	// Message carries outcome details (waiting reason or failure text).
	Message string `json:"message,omitempty"`
	// Plan is the plan snapshot after processing the receipt.
	Plan Plan `json:"plan"`
}

// RegisterShardRequest creates a shard at configuration version 1.
type RegisterShardRequest struct {
	ShardID    ShardID     `json:"shard_id"`
	Primary    ReplicaID   `json:"primary"`
	Members    []ReplicaID `json:"members"`
	MinHealthy int         `json:"min_healthy"`
}

// CreatePlanRequest freezes the starting configuration and the target.
type CreatePlanRequest struct {
	ShardID ShardID `json:"shard_id"`
	// TargetMembers is the desired full replica set; it must keep at least
	// MinHealthy replicas healthy at all times.
	TargetMembers []ReplicaID `json:"target_members"`
	// TargetPrimary is optional; it defaults to the current primary and
	// must be one of TargetMembers.
	TargetPrimary ReplicaID `json:"target_primary,omitempty"`
}

// UpdateConfigRequest applies a manual configuration change. The change
// must keep MinHealthy healthy replicas and invalidates any plan frozen on
// the previous version (that plan pauses with a conflict).
type UpdateConfigRequest struct {
	ShardID    ShardID     `json:"shard_id"`
	Primary    ReplicaID   `json:"primary"`
	Members    []ReplicaID `json:"members"`
	MinHealthy int         `json:"min_healthy"`
	Reason     string      `json:"reason,omitempty"`
}

// ReportHealthRequest replaces the observed healthy replica set. It never
// changes the configuration version; when health recovers, a plan waiting
// for healthy replicas advances automatically.
type ReportHealthRequest struct {
	ShardID ShardID     `json:"shard_id"`
	Healthy []ReplicaID `json:"healthy"`
}

// HistoryEntry is one persisted record: a lifecycle event or a
// configuration version change. Detail holds the typed payload (see the
// As* accessors).
type HistoryEntry struct {
	Seq           int64   `json:"seq"`
	Kind          string  `json:"kind"`
	ShardID       ShardID `json:"shard_id,omitempty"`
	PlanID        string  `json:"plan_id,omitempty"`
	ConfigVersion int64   `json:"config_version,omitempty"`
	At            string  `json:"at"`
	Detail        any     `json:"detail"`
}

// AsShardRegistered returns the typed detail, or nil for other kinds.
func (e HistoryEntry) AsShardRegistered() *ShardRegisteredDetail {
	if e.Kind == KindShardRegistered {
		if d, ok := e.Detail.(*ShardRegisteredDetail); ok {
			return d
		}
	}
	return nil
}

// AsHealthReported returns the typed detail, or nil for other kinds.
func (e HistoryEntry) AsHealthReported() *HealthReportedDetail {
	if e.Kind == KindHealthReported {
		if d, ok := e.Detail.(*HealthReportedDetail); ok {
			return d
		}
	}
	return nil
}

// AsConfigChange returns the typed detail, or nil for other kinds.
func (e HistoryEntry) AsConfigChange() *ConfigChangeDetail {
	if e.Kind == KindConfigChanged {
		if d, ok := e.Detail.(*ConfigChangeDetail); ok {
			return d
		}
	}
	return nil
}

// AsPlanCreated returns the typed detail, or nil for other kinds.
func (e HistoryEntry) AsPlanCreated() *PlanCreatedDetail {
	if e.Kind == KindPlanCreated {
		if d, ok := e.Detail.(*PlanCreatedDetail); ok {
			return d
		}
	}
	return nil
}

// AsStepEntered returns the typed detail, or nil for other kinds.
func (e HistoryEntry) AsStepEntered() *StepEnteredDetail {
	if e.Kind == KindStepEntered {
		if d, ok := e.Detail.(*StepEnteredDetail); ok {
			return d
		}
	}
	return nil
}

// AsReceipt returns the typed detail, or nil for other kinds.
func (e HistoryEntry) AsReceipt() *ReceiptRecordedDetail {
	if e.Kind == KindReceiptRecorded {
		if d, ok := e.Detail.(*ReceiptRecordedDetail); ok {
			return d
		}
	}
	return nil
}

// AsStepAdvanced returns the typed detail, or nil for other kinds.
func (e HistoryEntry) AsStepAdvanced() *StepAdvancedDetail {
	if e.Kind == KindStepAdvanced {
		if d, ok := e.Detail.(*StepAdvancedDetail); ok {
			return d
		}
	}
	return nil
}

// AsPlanPaused returns the typed detail, or nil for other kinds.
func (e HistoryEntry) AsPlanPaused() *PlanPausedDetail {
	if e.Kind == KindPlanPaused {
		if d, ok := e.Detail.(*PlanPausedDetail); ok {
			return d
		}
	}
	return nil
}

// AsPlanResumed returns the typed detail, or nil for other kinds.
func (e HistoryEntry) AsPlanResumed() *PlanResumedDetail {
	if e.Kind == KindPlanResumed {
		if d, ok := e.Detail.(*PlanResumedDetail); ok {
			return d
		}
	}
	return nil
}

// AsPlanCancelled returns the typed detail, or nil for other kinds.
func (e HistoryEntry) AsPlanCancelled() *PlanCancelledDetail {
	if e.Kind == KindPlanCancelled {
		if d, ok := e.Detail.(*PlanCancelledDetail); ok {
			return d
		}
	}
	return nil
}

// AsPlanCompleted returns the typed detail, or nil for other kinds.
func (e HistoryEntry) AsPlanCompleted() *PlanCompletedDetail {
	if e.Kind == KindPlanCompleted {
		if d, ok := e.Detail.(*PlanCompletedDetail); ok {
			return d
		}
	}
	return nil
}

func cloneIDs(ids []ReplicaID) []ReplicaID {
	if ids == nil {
		return nil
	}
	out := make([]ReplicaID, len(ids))
	copy(out, ids)
	return out
}
