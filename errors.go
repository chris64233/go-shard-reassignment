package goshardreassignment

import "errors"

var (
	// ErrConflict 表示请求与已有任务冲突（参数变化或分片已有有效迁移）。
	ErrConflict = errors.New("migration conflict")
	// ErrNotFound 表示迁移任务或分片不存在。
	ErrNotFound = errors.New("migration not found")
	// ErrStaleVersion 表示回执携带的迁移版本落后于冻结版本。
	ErrStaleVersion = errors.New("stale migration version")
	// ErrWatermarkRegression 表示回执检查点相对已确认水位倒退。
	ErrWatermarkRegression = errors.New("checkpoint watermark regression")
	// ErrOutOfRange 表示回执检查点超出冻结的复制范围。
	ErrOutOfRange = errors.New("checkpoint out of frozen range")
	// ErrNodeUnavailable 表示目标节点失联，可重试或暂停。
	ErrNodeUnavailable = errors.New("target node unavailable")
	// ErrInvalidPhase 表示当前阶段下不允许该操作。
	ErrInvalidPhase = errors.New("invalid migration phase")
	// ErrLeaseMismatch 表示切换确认携带的租约与当前有效租约不一致。
	ErrLeaseMismatch = errors.New("cutover lease mismatch")
	// ErrMigrationClosed 表示迁移已进入终态，迟到回执被丢弃。
	ErrMigrationClosed = errors.New("migration already closed")
	// ErrInvalidRequest 表示请求参数非法。
	ErrInvalidRequest = errors.New("invalid migration request")
	// ErrTargetViable 表示原目标仍可继续，不满足重新规划的前提。
	ErrTargetViable = errors.New("replan requires target to be clearly unable to continue")
	// ErrDigestMismatch 表示新目标无法证明复用区间数据一致。
	ErrDigestMismatch = errors.New("reuse digest mismatch")
	// ErrNoPendingReplan 表示迁移上没有待确认的重新规划。
	ErrNoPendingReplan = errors.New("no pending replan")
)
