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
	// PhaseSuperseded 已被重新规划取代：旧目标停止领取，迟到回执只进历史。
	PhaseSuperseded Phase = "SUPERSEDED"
)

// Terminal 报告阶段是否为终态（不再接受任何推进）。
func (p Phase) Terminal() bool {
	return p == PhaseCompleted || p == PhaseFailed || p == PhaseSuperseded
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

// ReplanRequest 是迁移途中重新规划目标节点的请求。RequestID 为幂等键：
// 相同重新规划号且内容一致返回原结果；检查点、候选目标或迁移版本变化
// 返回 ErrConflict。ReuseCheckpoint 与 Digest 共同构成进度复用依据：
// 只有不超过原迁移已确认水位、且摘要可被新目标证明一致的部分才能复用。
type ReplanRequest struct {
	RequestID       string
	MigrationID     string
	NewTarget       string
	Reason          string
	ReuseCheckpoint uint64
	Digest          string
}

// 重新规划记录的状态。
const (
	ReplanPending   = "PENDING"
	ReplanConfirmed = "CONFIRMED"
	ReplanAborted   = "ABORTED"
)

// Replan 是一次重新规划的持久化记录，申请时冻结原迁移版本、已确认
// 检查点、原目标、候选新目标与失败原因。
type Replan struct {
	RequestID       string    `json:"request_id"`
	MigrationID     string    `json:"migration_id"`
	ShardID         string    `json:"shard_id"`
	Version         uint64    `json:"version"`
	OldTarget       string    `json:"old_target"`
	NewTarget       string    `json:"new_target"`
	Checkpoint      uint64    `json:"checkpoint"`
	ReuseCheckpoint uint64    `json:"reuse_checkpoint"`
	Digest          string    `json:"digest"`
	Reason          string    `json:"reason"`
	State           string    `json:"state"`
	NewMigrationID  string    `json:"new_migration_id,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
}

// sameContent 判断重新规划记录与请求内容是否一致（幂等/冲突判定）。
func (r *Replan) sameContent(req ReplanRequest, task *Task) bool {
	return r.MigrationID == req.MigrationID &&
		r.Version == task.Version &&
		r.NewTarget == req.NewTarget &&
		r.Reason == req.Reason &&
		r.ReuseCheckpoint == req.ReuseCheckpoint &&
		r.Digest == req.Digest
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
	ReplanOf        string    `json:"replan_of,omitempty"`
	ReplannedBy     string    `json:"replanned_by,omitempty"`
	ReuseCheckpoint uint64    `json:"reuse_checkpoint,omitempty"`
	ReuseDigest     string    `json:"reuse_digest,omitempty"`
	PendingReplan   *Replan   `json:"pending_replan,omitempty"`
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
	EventMigrationStarted    = "migration_started"
	EventReceiptAccepted     = "receipt_accepted"
	EventReceiptRejected     = "receipt_rejected"
	EventCaughtUp            = "caught_up"
	EventCutoverStarted      = "cutover_started"
	EventCutoverConfirmed    = "cutover_confirmed"
	EventCutoverAborted      = "cutover_aborted"
	EventMigrationFailed     = "migration_failed"
	EventNodeDown            = "node_down"
	EventNodeUp              = "node_up"
	EventRecovered           = "recovered_after_restart"
	EventOwnershipAssigned   = "ownership_assigned"
	EventReplanRequested     = "replan_requested"
	EventReplanConfirmed     = "replan_confirmed"
	EventReplanAborted       = "replan_aborted"
	EventMigrationSuperseded = "migration_superseded"
	EventLateReceipt         = "late_receipt_recorded"
	EventLateConfirm         = "late_cutover_confirm_recorded"
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
	ReplanOf        string
	ReplannedBy     string
	ReuseCheckpoint uint64
	ReuseDigest     string
	PendingReplan   *Replan
	Audit           []AuditEntry
}
