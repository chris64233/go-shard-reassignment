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
	// PhaseSuperseded 已被取代：重新规划确认后旧迁移进入该终态，
	// 旧目标停止领取，迟到回执只能进入历史，不能改变正式归属。
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
	// Digest 是截至 Checkpoint 的数据摘要，用于重新规划时验证
	// 新目标已拥有的数据与已确认检查点一致。
	Digest string
}

// ReplanRequest 是迁移途中重新规划目标节点的请求。RequestID 为重新
// 规划号，用于幂等去重：相同重新规划号和内容返回原结果，检查点、
// 候选目标或迁移版本变化返回冲突。
type ReplanRequest struct {
	RequestID   string
	MigrationID string
	Reason      string
	Candidates  []string
}

// ReuseProof 是新目标提交的进度复用证明：声称自己拥有截至 Checkpoint
// 且摘要为 Digest 的数据。只有与已确认检查点的摘要一致时才会被采信，
// 否则从安全位置（冻结范围起点）重新复制。
type ReuseProof struct {
	Checkpoint uint64 `json:"checkpoint"`
	Digest     string `json:"digest"`
}

// ReuseBasis 记录新版本迁移的进度复用依据，随任务持久化并可查询。
type ReuseBasis struct {
	Proof      ReuseProof `json:"proof"`
	Verified   bool       `json:"verified"`
	ReusedFrom uint64     `json:"reused_from"`
}

// Replan 是一次重新规划的持久化记录，冻结原迁移版本、已确认检查点、
// 原目标、候选新目标及失败原因。
type Replan struct {
	ID              string            `json:"id"`
	RequestID       string            `json:"request_id"`
	MigrationID     string            `json:"migration_id"`
	ShardID         string            `json:"shard_id"`
	FrozenVersion   uint64            `json:"frozen_version"`
	FrozenWatermark uint64            `json:"frozen_watermark"`
	FrozenDigests   map[uint64]string `json:"frozen_digests,omitempty"`
	OriginalTarget  string            `json:"original_target"`
	Candidates      []string          `json:"candidates"`
	Reason          string            `json:"reason"`
	NewMigrationID  string            `json:"new_migration_id,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
}

// sameParams 判断重新规划冻结的内容是否与请求一致（幂等/冲突判定）。
func (r *Replan) sameParams(req ReplanRequest, task *Task) bool {
	if r.MigrationID != req.MigrationID || r.Reason != req.Reason ||
		r.FrozenVersion != task.Version || r.FrozenWatermark != task.CopyWatermark ||
		len(r.Candidates) != len(req.Candidates) {
		return false
	}
	for i := range r.Candidates {
		if r.Candidates[i] != req.Candidates[i] {
			return false
		}
	}
	return true
}

// Task 是一次迁移任务的持久化记录，每次变更整体落盘。
type Task struct {
	ID              string `json:"id"`
	RequestID       string `json:"request_id"`
	ShardID         string `json:"shard_id"`
	Source          string `json:"source"`
	Target          string `json:"target"`
	Version         uint64 `json:"version"`
	FromCheckpoint  uint64 `json:"from_checkpoint"`
	ToCheckpoint    uint64 `json:"to_checkpoint"`
	Phase           Phase  `json:"phase"`
	CopyWatermark   uint64 `json:"copy_watermark"`
	TargetWatermark uint64 `json:"target_watermark"`
	// Digests 记录每个已确认检查点对应的数据摘要，是重新规划时
	// 判定新目标可复用进度的唯一依据。
	Digests map[uint64]string `json:"digests,omitempty"`
	// ReplanOf 指向被本次重新规划取代的旧迁移，形成版本关系链。
	ReplanOf        string `json:"replan_of,omitempty"`
	ReplanRequestID string `json:"replan_request_id,omitempty"`
	// Reuse 记录本次迁移起点的复用依据（仅重新规划产生的新版本存在）。
	Reuse         *ReuseBasis `json:"reuse,omitempty"`
	LeaseToken    string      `json:"lease_token,omitempty"`
	Attempts      int         `json:"attempts"`
	FailureReason string      `json:"failure_reason,omitempty"`
	CreatedAt     time.Time   `json:"created_at"`
	UpdatedAt     time.Time   `json:"updated_at"`
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
	EventMigrationSuperseded = "migration_superseded"
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
	// PreviousMigrationID 指向被取代的旧迁移（版本关系链）。
	PreviousMigrationID string
	// FinalTarget 是迁移链最终的切换目标节点。
	FinalTarget string
	// Reuse 是新版本迁移的进度复用依据（重新规划产生）。
	Reuse *ReuseBasis
	Audit []AuditEntry
}
