package goshardreassignment

import "time"

// State 表示迁移任务所处的阶段。正式归属只在 StateCompleted 之后变化。
type State string

const (
	// StateCopying 复制中：已冻结源/目标/版本/检查点范围，正在复制。
	StateCopying State = "COPYING"
	// StateAwaitingCutover 追平待切换：复制水位已达到冻结范围上限。
	StateAwaitingCutover State = "AWAITING_CUTOVER"
	// StateCuttingOver 切换中：已发出切换租约，等待切换确认。
	StateCuttingOver State = "CUTTING_OVER"
	// StateCompleted 已完成：切换确认成功，正式归属已改写。
	StateCompleted State = "COMPLETED"
	// StateFailed 已失败：回滚到原归属，失败原因与已确认进度保留可查。
	StateFailed State = "FAILED"
)

// Terminal 报告状态是否为终态。
func (s State) Terminal() bool {
	return s == StateCompleted || s == StateFailed
}

// StartRequest 是发起迁移的请求。RequestID 用于幂等去重。
type StartRequest struct {
	RequestID      string
	ShardID        string
	Source         string
	Target         string
	Version        uint64
	FromCheckpoint uint64
	ToCheckpoint   uint64
}

// Receipt 是目标节点上报的复制回执，必须携带迁移版本与单调检查点。
type Receipt struct {
	MigrationID string
	ShardID     string
	NodeID      string
	Version     uint64
	Checkpoint  uint64
}

// Task 是一次迁移任务的持久化记录。
type Task struct {
	ID              string    `json:"id"`
	RequestID       string    `json:"request_id"`
	ShardID         string    `json:"shard_id"`
	Source          string    `json:"source"`
	Target          string    `json:"target"`
	Version         uint64    `json:"version"`
	FromCheckpoint  uint64    `json:"from_checkpoint"`
	ToCheckpoint    uint64    `json:"to_checkpoint"`
	State           State     `json:"state"`
	CopyWatermark   uint64    `json:"copy_watermark"`
	TargetWatermark uint64    `json:"target_watermark"`
	LeaseToken      string    `json:"lease_token,omitempty"`
	Attempts        int       `json:"attempts"`
	FailureReason   string    `json:"failure_reason,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// matchesRequest 判断任务是否与给定请求参数一致（用于幂等/冲突判定）。
func (t *Task) matchesRequest(req StartRequest) bool {
	return t.ShardID == req.ShardID &&
		t.Source == req.Source &&
		t.Target == req.Target &&
		t.Version == req.Version &&
		t.FromCheckpoint == req.FromCheckpoint &&
		t.ToCheckpoint == req.ToCheckpoint
}

// AuditEntry 是一条审计历史记录。
type AuditEntry struct {
	Time        time.Time `json:"time"`
	ShardID     string    `json:"shard_id"`
	MigrationID string    `json:"migration_id"`
	Event       string    `json:"event"`
	Detail      string    `json:"detail,omitempty"`
}

// 审计事件名。
const (
	EventMigrationStarted  = "migration_started"
	EventReceiptAccepted   = "receipt_accepted"
	EventReceiptRejected   = "receipt_rejected"
	EventCaughtUp          = "caught_up"
	EventCutoverStarted    = "cutover_started"
	EventCutoverConfirmed  = "cutover_confirmed"
	EventCutoverAborted    = "cutover_aborted"
	EventMigrationFailed   = "migration_failed"
	EventNodeDown          = "node_down"
	EventNodeUp            = "node_up"
	EventRecovered         = "recovered_after_restart"
	EventOwnershipAssigned = "ownership_assigned"
)

// Status 是分片迁移状态的查询视图。
type Status struct {
	ShardID         string
	Owner           string
	HasMigration    bool
	MigrationID     string
	State           State
	Version         uint64
	Source          string
	Target          string
	FromCheckpoint  uint64
	ToCheckpoint    uint64
	CopyWatermark   uint64
	TargetWatermark uint64
	LeaseToken      string
	Attempts        int
	FailureReason   string
	Audit           []AuditEntry
}
