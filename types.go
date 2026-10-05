package goshardreassignment

import "time"

// Phase 表示迁移任务所处的阶段。正式归属（Owner）只在 PhaseCompleted
// 之后才会变化；在此之前的任何阶段，分片仍归源节点所有。
type Phase string

const (
	// PhaseCopying 复制中：源/目标/版本/检查点范围已冻结，正在复制。
	PhaseCopying Phase = "COPYING"
	// PhaseCaughtUp 追平待切换：复制水位已达到冻结范围上限，等待切换。
	PhaseCaughtUp Phase = "CAUGHT_UP"
	// PhaseCuttingOver 切换中：已签发切换租约，等待切换确认。
	PhaseCuttingOver Phase = "CUTTING_OVER"
	// PhaseCompleted 已完成：切换确认成功，正式归属已改写。
	PhaseCompleted Phase = "COMPLETED"
	// PhaseFailed 已失败：回滚到原归属，失败原因与已确认进度保留可查。
	PhaseFailed Phase = "FAILED"
)

// Terminal 报告阶段是否为终态（不再接受任何推进）。
func (p Phase) Terminal() bool {
	return p == PhaseCompleted || p == PhaseFailed
}

// StartRequest 是发起迁移的请求。RequestID 用于幂等去重：
// 同一 RequestID 重复提交返回原任务，参数变化则返回冲突。
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

// Task 是一次迁移任务的持久化记录，每次变更整体落盘。
type Task struct {
	ID              string    `json:"id"`
	RequestID       string    `json:"request_id"`
	ShardID         string    `json:"shard_id"`
	Source          string    `json:"source"`
	Target          string    `json:"target"`
	Version         uint64    `json:"version"`
	FromCheckpoint  uint64    `json:"from_checkpoint"`
	ToCheckpoint    uint64    `json:"to_checkpoint"`
	Phase           Phase     `json:"phase"`
	CopyWatermark   uint64    `json:"copy_watermark"`
	TargetWatermark uint64    `json:"target_watermark"`
	LeaseToken      string    `json:"lease_token,omitempty"`
	Attempts        int       `json:"attempts"`
	FailureReason   string    `json:"failure_reason,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// sameParams 判断任务冻结的参数是否与请求一致（幂等/冲突判定）。
func (t *Task) sameParams(req StartRequest) bool {
	return t.ShardID == req.ShardID &&
		t.Source == req.Source &&
		t.Target == req.Target &&
		t.Version == req.Version &&
		t.FromCheckpoint == req.FromCheckpoint &&
		t.ToCheckpoint == req.ToCheckpoint
}

// AuditEntry 是一条审计历史记录，随状态一起持久化。
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
	Phase           Phase
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
