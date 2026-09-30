package goshardreassignment

import "errors"

// Sentinel errors returned by the service. Callers can inspect them with
// errors.Is; human-readable context is attached with fmt.Errorf.
var (
	// ErrShardNotFound indicates that the shard has never been registered.
	ErrShardNotFound = errors.New("shard not found")
	// ErrShardExists indicates that a shard with the same ID is registered.
	ErrShardExists = errors.New("shard already registered")
	// ErrInvalidRequest indicates a malformed request (empty identifiers,
	// duplicate members, nil payload, target identical to current, ...).
	ErrInvalidRequest = errors.New("invalid request")
	// ErrSafetyViolation indicates that the operation would leave fewer
	// than the configured minimum number of healthy replicas. The operation
	// is rejected and no configuration version change is persisted.
	ErrSafetyViolation = errors.New("minimum healthy replica requirement violated")
	// ErrPlanActive indicates that a non-terminal plan already exists for
	// the shard; a shard can only have one active migration at a time.
	ErrPlanActive = errors.New("an active plan already exists for shard")
	// ErrPlanNotFound indicates that no plan with the given ID exists.
	ErrPlanNotFound = errors.New("plan not found")
	// ErrPlanConflict indicates that the plan was frozen on a configuration
	// version that is no longer current. Such a plan can only be cancelled;
	// it must never overwrite the newer configuration.
	ErrPlanConflict = errors.New("plan conflicts with newer configuration")
	// ErrPlanNotActive indicates that the plan is terminal (completed or
	// cancelled) and no longer accepts control or receipt operations.
	ErrPlanNotActive = errors.New("plan is not active")
	// ErrPlanPaused indicates that the plan is paused and cannot accept
	// receipts until it is resumed.
	ErrPlanPaused = errors.New("plan is paused")
	// ErrStaleReceipt indicates that the receipt refers to an old step or
	// an old configuration version. Late receipts are recorded nowhere and
	// can never skip or advance the current step.
	ErrStaleReceipt = errors.New("stale receipt for previous step or configuration version")
	// ErrPrimaryMissing indicates that the primary is not part of members.
	ErrPrimaryMissing = errors.New("primary must be a member")
)
