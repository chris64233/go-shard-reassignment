package goshardreassignment

// ReplicaState describes whether a replica is considered healthy.
type ReplicaState string

const (
	ReplicaHealthy   ReplicaState = "healthy"
	ReplicaUnhealthy ReplicaState = "unhealthy"
)

// Replica is one member of a shard's replica set.
type Replica struct {
	NodeID    string       `json:"nodeId"`
	Primary   bool         `json:"primary"`
	State     ReplicaState `json:"state"`
}

// ShardConfig is an immutable snapshot of a shard configuration at one version.
type ShardConfig struct {
	ShardID      string    `json:"shardId"`
	Version      int64     `json:"version"`
	PrimaryID    string    `json:"primaryId"`
	Replicas     []Replica `json:"replicas"`
	MinHealthy   int       `json:"minHealthy"`
}

// HealthyCount returns how many replicas are currently healthy.
func (c ShardConfig) HealthyCount() int {
	n := 0
	for _, r := range c.Replicas {
		if r.State == ReplicaHealthy {
			n++
		}
	}
	return n
}

// HasReplica reports whether nodeID is a member of the replica set.
func (c ShardConfig) HasReplica(nodeID string) bool {
	for _, r := range c.Replicas {
		if r.NodeID == nodeID {
			return true
		}
	}
	return false
}

// Op is the kind of mutation performed by a plan step.
type Op string

const (
	// OpAdd installs a target replica and starts replication.
	OpAdd Op = "add"
	// OpSync waits until a target replica has caught up with the primary.
	OpSync Op = "sync"
	// OpPromote transfers the primary role to a target replica.
	OpPromote Op = "promote"
	// OpRemove drops an old replica from the set.
	OpRemove Op = "remove"
)

// PlanStatus is the lifecycle state of a migration plan.
type PlanStatus string

const (
	// PlanRunning is the normal state: the plan waits for receipts.
	PlanRunning PlanStatus = "running"
	// PlanPaused was paused manually without any conflict.
	PlanPaused PlanStatus = "paused"
	// PlanFailed pauses after a failing receipt; completed steps stay in place.
	PlanFailed PlanStatus = "failed"
	// PlanConflict pauses because a newer shard configuration invalidated it.
	PlanConflict PlanStatus = "conflict"
	// PlanCanceling rolls forward past an irreversible point on cancellation.
	PlanCanceling PlanStatus = "canceling"
	// PlanCompleted is a terminal state.
	PlanCompleted PlanStatus = "completed"
	// PlanCanceled is a terminal state.
	PlanCanceled PlanStatus = "canceled"
)

// IsTerminal reports whether the plan can no longer move.
func (s PlanStatus) IsTerminal() bool {
	return s == PlanCompleted || s == PlanCanceled
}

// Step is one frozen instruction of a migration plan.
type Step struct {
	Index       int    `json:"index"`
	Op          Op     `json:"op"`
	NodeID      string `json:"nodeId"`
	// BaseVersion is the config version the node must echo in its receipt.
	// Every step is addressed against an exact configuration version, so a
	// late receipt from an older step can never jump the plan forward.
	BaseVersion int64  `json:"baseVersion"`
	Done        bool   `json:"done"`
}

// Receipt is a node acknowledgement for a plan step.
type Receipt struct {
	PlanID        string `json:"planId"`
	StepIndex     int    `json:"stepIndex"`
	ConfigVersion int64  `json:"configVersion"`
	NodeID        string `json:"nodeId"`
	Success       bool   `json:"success"`
	Message       string `json:"message,omitempty"`
}

// Plan is a frozen migration together with its execution state.
type Plan struct {
	ID            string     `json:"id"`
	ShardID       string     `json:"shardId"`
	// StartConfig is the configuration frozen at plan creation.
	StartConfig   ShardConfig `json:"startConfig"`
	// TargetReplicas is the desired replica set with roles.
	TargetReplicas []Replica  `json:"targetReplicas"`
	TargetPrimary  string     `json:"targetPrimary"`
	MinHealthy     int        `json:"minHealthy"`
	Steps          []Step     `json:"steps"`
	CurrentStep    int        `json:"currentStep"`
	Status         PlanStatus `json:"status"`
	ConflictReason string     `json:"conflictReason,omitempty"`
	FailureReason  string     `json:"failureReason,omitempty"`
	CreatedAt      string     `json:"createdAt"`
	UpdatedAt      string     `json:"updatedAt"`
	// receipts caches the first processed receipt of every finished step so
	// duplicate receipts return the original result.
	receipts map[int]Receipt
}

// DoneSteps returns the number of completed steps.
func (p *Plan) DoneSteps() int {
	n := 0
	for _, s := range p.Steps {
		if s.Done {
			n++
		}
	}
	return n
}

// EventType enumerates history record kinds.
type EventType string

const (
	EventShardRegistered EventType = "shard_registered"
	EventConfigChanged   EventType = "config_changed"
	EventPlanCreated     EventType = "plan_created"
	EventStepSucceeded   EventType = "step_succeeded"
	EventStepFailed      EventType = "step_failed"
	EventPlanPaused      EventType = "plan_paused"
	EventPlanResumed     EventType = "plan_resumed"
	EventPlanConflict    EventType = "plan_conflict"
	EventPlanCanceling   EventType = "plan_canceling"
	EventPlanCompleted   EventType = "plan_completed"
	EventPlanCanceled    EventType = "plan_canceled"
)

// Event is one persisted history entry. ConfigVersion records the version
// resulting from the change (0 for events that do not mutate configuration).
type Event struct {
	ID            int64         `json:"id"`
	Time          string        `json:"time"`
	Type          EventType     `json:"type"`
	ShardID       string        `json:"shardId"`
	PlanID        string        `json:"planId,omitempty"`
	StepIndex     int           `json:"stepIndex,omitempty"`
	Op            Op            `json:"op,omitempty"`
	ConfigVersion int64         `json:"configVersion,omitempty"`
	Message       string        `json:"message,omitempty"`
	Config        *ShardConfig  `json:"config,omitempty"`
}
