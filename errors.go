package goshardreassignment

import "errors"

// Sentinel errors returned by the manager. HTTP transport and callers can use
// errors.Is to map them to status codes or specific handling.
var (
	ErrNotFound            = errors.New("shard or plan not found")
	ErrAlreadyExists       = errors.New("shard already exists")
	ErrInvalidArgument     = errors.New("invalid argument")
	ErrActivePlanExists    = errors.New("an active plan already exists for shard")
	ErrPlanNotActive       = errors.New("plan is not accepting receipts")
	ErrStaleReceipt        = errors.New("stale receipt: old step or old config version")
	ErrConflict            = errors.New("plan conflicts with newer shard configuration")
	ErrSafetyViolation     = errors.New("operation would violate minimum healthy replica count")
	ErrInvalidTransition    = errors.New("invalid plan state transition")
)
