package goshardreassignment

import (
	"fmt"
	"sync"
	"time"
)

// Manager 管理分片迁移的全生命周期。所有变更都在单把互斥锁下完成并
// 整体原子落盘，因此切换、节点下线、迁移失败与崩溃恢复并发时，只会
// 有一个有效结果被持久化。
type Manager struct {
	mu    sync.Mutex
	store Store
	state *persistedState
	nodes map[string]bool // nodeID -> 是否在线（未知节点默认在线）
	now   func() time.Time
}

// NewManager 加载持久化状态并执行崩溃恢复：任何停留在“切换中”的任务
// 说明上次切换未被确认，安全回退到“追平待切换”并作废租约，避免重启后
// 重复切换；已确认的复制水位与检查点范围保持不变。
func NewManager(store Store) (*Manager, error) {
	state, err := store.Load()
	if err != nil {
		return nil, err
	}
	m := &Manager{
		store: store,
		state: state,
		nodes: map[string]bool{},
		now:   time.Now,
	}
	dirty := false
	for _, t := range m.state.Tasks {
		if t.Phase == PhaseCuttingOver {
			t.Phase = PhaseCaughtUp
			t.LeaseToken = ""
			t.UpdatedAt = m.now()
			m.auditLocked(t, EventRecovered,
				"unconfirmed cutover discarded after restart; watermark preserved")
			dirty = true
		}
	}
	if dirty {
		if err := m.store.Save(m.state); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// RegisterShard 登记分片当前归属，用于迁移前的初始拓扑。
func (m *Manager) RegisterShard(shardID, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if shardID == "" || owner == "" {
		return fmt.Errorf("%w: shard and owner are required", ErrInvalidRequest)
	}
	m.state.Owners[shardID] = owner
	m.auditLocked(&Task{ShardID: shardID}, EventOwnershipAssigned,
		fmt.Sprintf("shard %s registered to %s", shardID, owner))
	return m.store.Save(m.state)
}

// SetNodeUp 标记节点恢复在线。
func (m *Manager) SetNodeUp(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[nodeID] = true
	m.auditLocked(&Task{}, EventNodeUp, nodeID)
	return m.store.Save(m.state)
}

// SetNodeDown 标记节点失联。以该节点为目标且正处于“切换中”的任务会被
// 安全回退到“追平待切换”（租约作废），已确认水位不受影响；复制可以
// 重试或暂停，但绝不会把部分复制误报为完成。
func (m *Manager) SetNodeDown(nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[nodeID] = false
	m.auditLocked(&Task{}, EventNodeDown, nodeID)
	for _, t := range m.state.Tasks {
		if t.Phase == PhaseCuttingOver && t.Target == nodeID {
			t.Phase = PhaseCaughtUp
			t.LeaseToken = ""
			t.UpdatedAt = m.now()
			m.auditLocked(t, EventCutoverAborted,
				fmt.Sprintf("target %s went down during cutover", nodeID))
		}
	}
	return m.store.Save(m.state)
}

func (m *Manager) nodeUp(nodeID string) bool {
	up, known := m.nodes[nodeID]
	return !known || up
}

// StartMigration 发起迁移并冻结源、目标、版本与检查点范围。
// 相同 RequestID 重复提交返回原任务；参数变化返回 ErrConflict；
// 同一分片存在另一条有效（非终态）迁移时返回 ErrConflict。
func (m *Manager) StartMigration(req StartRequest) (*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.RequestID == "" || req.ShardID == "" || req.Source == "" || req.Target == "" {
		return nil, fmt.Errorf("%w: request_id/shard/source/target are required", ErrInvalidRequest)
	}
	if req.Source == req.Target {
		return nil, fmt.Errorf("%w: source and target must differ", ErrInvalidRequest)
	}
	if req.ToCheckpoint < req.FromCheckpoint {
		return nil, fmt.Errorf("%w: checkpoint range is inverted", ErrInvalidRequest)
	}
	if id, ok := m.state.Requests[req.RequestID]; ok {
		existing := m.state.Tasks[id]
		if !existing.sameParams(req) {
			return nil, fmt.Errorf("%w: request %s reused with different parameters", ErrConflict, req.RequestID)
		}
		dup := *existing
		return &dup, nil
	}
	for _, t := range m.state.Tasks {
		if t.ShardID == req.ShardID && !t.Phase.Terminal() {
			return nil, fmt.Errorf("%w: shard %s already has active migration %s", ErrConflict, req.ShardID, t.ID)
		}
	}
	owner := m.state.Owners[req.ShardID]
	if owner != "" && owner != req.Source {
		return nil, fmt.Errorf("%w: shard %s is owned by %s, not %s", ErrConflict, req.ShardID, owner, req.Source)
	}
	if owner == "" {
		m.state.Owners[req.ShardID] = req.Source
	}
	now := m.now()
	task := &Task{
		ID:              "mig-" + req.RequestID,
		RequestID:       req.RequestID,
		ShardID:         req.ShardID,
		Source:          req.Source,
		Target:          req.Target,
		Version:         req.Version,
		FromCheckpoint:  req.FromCheckpoint,
		ToCheckpoint:    req.ToCheckpoint,
		Phase:           PhaseCopying,
		CopyWatermark:   req.FromCheckpoint,
		TargetWatermark: req.FromCheckpoint,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	m.state.Tasks[task.ID] = task
	m.state.Requests[req.RequestID] = task.ID
	m.auditLocked(task, EventMigrationStarted,
		fmt.Sprintf("frozen source=%s target=%s version=%d range=[%d,%d]",
			req.Source, req.Target, req.Version, req.FromCheckpoint, req.ToCheckpoint))
	if err := m.store.Save(m.state); err != nil {
		return nil, err
	}
	dup := *task
	return &dup, nil
}

// SubmitReceipt 处理复制回执。重复回执幂等；旧版本、倒退水位、超出
// 冻结范围的回执被拒绝并记入审计；目标节点失联时拒绝确认（可重试），
// 不会把部分复制误报为完成。
func (m *Manager) SubmitReceipt(r Receipt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.state.Tasks[r.MigrationID]
	if !ok || task.ShardID != r.ShardID {
		return fmt.Errorf("%w: %s", ErrNotFound, r.MigrationID)
	}
	if r.Version != task.Version {
		m.auditLocked(task, EventReceiptRejected,
			fmt.Sprintf("stale version %d (frozen %d)", r.Version, task.Version))
		_ = m.store.Save(m.state)
		return fmt.Errorf("%w: got %d, frozen %d", ErrStaleVersion, r.Version, task.Version)
	}
	if task.Phase.Terminal() {
		m.auditLocked(task, EventLateReceipt,
			fmt.Sprintf("late receipt checkpoint %d from %s on %s migration recorded to history",
				r.Checkpoint, r.NodeID, task.Phase))
		_ = m.store.Save(m.state)
		return fmt.Errorf("%w: %s is %s", ErrMigrationClosed, task.ID, task.Phase)
	}
	if task.Phase == PhaseCuttingOver {
		return fmt.Errorf("%w: cutover in progress, retry later", ErrInvalidPhase)
	}
	if !m.nodeUp(task.Target) {
		return fmt.Errorf("%w: %s is down", ErrNodeUnavailable, task.Target)
	}
	if r.Checkpoint < task.CopyWatermark {
		m.auditLocked(task, EventReceiptRejected,
			fmt.Sprintf("regressed checkpoint %d < watermark %d", r.Checkpoint, task.CopyWatermark))
		_ = m.store.Save(m.state)
		return fmt.Errorf("%w: %d < %d", ErrWatermarkRegression, r.Checkpoint, task.CopyWatermark)
	}
	if r.Checkpoint > task.ToCheckpoint {
		m.auditLocked(task, EventReceiptRejected,
			fmt.Sprintf("checkpoint %d beyond frozen range %d", r.Checkpoint, task.ToCheckpoint))
		_ = m.store.Save(m.state)
		return fmt.Errorf("%w: %d > %d", ErrOutOfRange, r.Checkpoint, task.ToCheckpoint)
	}
	if r.Checkpoint == task.CopyWatermark {
		return nil // 重复回执：幂等成功
	}
	task.CopyWatermark = r.Checkpoint
	if r.Checkpoint > task.TargetWatermark {
		task.TargetWatermark = r.Checkpoint
	}
	task.UpdatedAt = m.now()
	m.auditLocked(task, EventReceiptAccepted,
		fmt.Sprintf("checkpoint %d confirmed by %s", r.Checkpoint, r.NodeID))
	if task.CopyWatermark >= task.ToCheckpoint && task.Phase == PhaseCopying {
		task.Phase = PhaseCaughtUp
		m.auditLocked(task, EventCaughtUp,
			fmt.Sprintf("reached frozen range end %d", task.ToCheckpoint))
	}
	return m.store.Save(m.state)
}

// BeginCutover 在追平后发起一次切换尝试，返回本次租约令牌。
// 目标节点失联或水位未达冻结范围上限时拒绝。
func (m *Manager) BeginCutover(migrationID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.state.Tasks[migrationID]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrNotFound, migrationID)
	}
	if task.Phase != PhaseCaughtUp {
		return "", fmt.Errorf("%w: %s is %s", ErrInvalidPhase, migrationID, task.Phase)
	}
	if task.CopyWatermark < task.ToCheckpoint {
		return "", fmt.Errorf("%w: watermark %d below frozen range %d",
			ErrInvalidPhase, task.CopyWatermark, task.ToCheckpoint)
	}
	if !m.nodeUp(task.Target) {
		return "", fmt.Errorf("%w: %s is down", ErrNodeUnavailable, task.Target)
	}
	task.Attempts++
	task.LeaseToken = fmt.Sprintf("%s/attempt-%d", task.ID, task.Attempts)
	task.Phase = PhaseCuttingOver
	task.UpdatedAt = m.now()
	m.auditLocked(task, EventCutoverStarted,
		fmt.Sprintf("attempt %d lease %s", task.Attempts, task.LeaseToken))
	if err := m.store.Save(m.state); err != nil {
		return "", err
	}
	return task.LeaseToken, nil
}

// ConfirmCutover 确认切换。只有租约匹配且任务处于“切换中”时才改写
// 正式归属，归属改写与任务完成在同一持久化事务内提交。使用相同租约
// 重复确认是幂等的，不会重复切换。
func (m *Manager) ConfirmCutover(migrationID, leaseToken string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.state.Tasks[migrationID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, migrationID)
	}
	if task.Phase == PhaseCompleted && task.LeaseToken == leaseToken {
		return nil // 幂等确认：归属已改写，不重复切换
	}
	if task.Phase.Terminal() {
		m.auditLocked(task, EventLateConfirm,
			fmt.Sprintf("late cutover confirm on %s migration recorded to history", task.Phase))
		_ = m.store.Save(m.state)
		return fmt.Errorf("%w: %s is %s", ErrMigrationClosed, task.ID, task.Phase)
	}
	if task.Phase != PhaseCuttingOver {
		return fmt.Errorf("%w: %s is %s", ErrInvalidPhase, migrationID, task.Phase)
	}
	if task.LeaseToken == "" || task.LeaseToken != leaseToken {
		return fmt.Errorf("%w: stale lease for %s", ErrLeaseMismatch, migrationID)
	}
	m.state.Owners[task.ShardID] = task.Target
	task.Phase = PhaseCompleted
	task.UpdatedAt = m.now()
	m.auditLocked(task, EventCutoverConfirmed,
		fmt.Sprintf("ownership %s -> %s at checkpoint %d", task.Source, task.Target, task.CopyWatermark))
	return m.store.Save(m.state)
}

// AbortCutover 放弃一次进行中的切换尝试，回退到“追平待切换”。
func (m *Manager) AbortCutover(migrationID, leaseToken, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.state.Tasks[migrationID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, migrationID)
	}
	if task.Phase != PhaseCuttingOver {
		return fmt.Errorf("%w: %s is %s", ErrInvalidPhase, migrationID, task.Phase)
	}
	if task.LeaseToken == "" || task.LeaseToken != leaseToken {
		return fmt.Errorf("%w: stale lease for %s", ErrLeaseMismatch, migrationID)
	}
	task.Phase = PhaseCaughtUp
	task.LeaseToken = ""
	task.UpdatedAt = m.now()
	m.auditLocked(task, EventCutoverAborted, reason)
	return m.store.Save(m.state)
}

// FailMigration 将迁移标记为失败。正式归属从未改变（保持源节点），
// 已确认的复制进度与失败原因保留可查。重复失败调用幂等。
func (m *Manager) FailMigration(migrationID, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.state.Tasks[migrationID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, migrationID)
	}
	if task.Phase == PhaseCompleted {
		return fmt.Errorf("%w: %s already completed", ErrConflict, migrationID)
	}
	if task.Phase == PhaseFailed {
		return nil
	}
	task.Phase = PhaseFailed
	task.LeaseToken = ""
	task.FailureReason = reason
	task.UpdatedAt = m.now()
	m.auditLocked(task, EventMigrationFailed, reason)
	return m.store.Save(m.state)
}

// RequestReplan 在迁移途中申请重新规划目标节点。前提：迁移尚未切换正式
// 归属（处于复制中或追平待切换）且原目标明确不可继续（已失联）。申请会
// 冻结原迁移版本、已确认检查点、原目标、候选新目标与失败原因。
// 复用进度必须满足：ReuseCheckpoint 不超过已确认水位（绝不采用原目标
// 上报的最高数字），并携带可验证摘要 Digest。相同 RequestID 且内容一致
// 返回原记录；检查点、候选目标或迁移版本变化返回 ErrConflict。
func (m *Manager) RequestReplan(req ReplanRequest) (*Replan, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.RequestID == "" || req.MigrationID == "" || req.NewTarget == "" ||
		req.Reason == "" || req.Digest == "" {
		return nil, fmt.Errorf("%w: request_id/migration/new_target/reason/digest are required", ErrInvalidRequest)
	}
	task, ok := m.state.Tasks[req.MigrationID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, req.MigrationID)
	}
	if existing, ok := m.state.Replans[req.RequestID]; ok {
		if !existing.sameContent(req, task) {
			return nil, fmt.Errorf("%w: replan %s reused with different checkpoint/candidate/version", ErrConflict, req.RequestID)
		}
		dup := *existing
		return &dup, nil
	}
	if task.Phase.Terminal() {
		return nil, fmt.Errorf("%w: %s is %s", ErrMigrationClosed, task.ID, task.Phase)
	}
	if task.Phase != PhaseCopying && task.Phase != PhaseCaughtUp {
		return nil, fmt.Errorf("%w: %s is %s", ErrInvalidPhase, task.ID, task.Phase)
	}
	if m.nodeUp(task.Target) {
		return nil, fmt.Errorf("%w: target %s is still reachable", ErrTargetViable, task.Target)
	}
	if task.PendingReplan != nil {
		return nil, fmt.Errorf("%w: %s already has pending replan %s", ErrConflict, task.ID, task.PendingReplan.RequestID)
	}
	if req.NewTarget == task.Source || req.NewTarget == task.Target {
		return nil, fmt.Errorf("%w: new target must differ from source and old target", ErrInvalidRequest)
	}
	if req.ReuseCheckpoint < task.FromCheckpoint || req.ReuseCheckpoint > task.CopyWatermark {
		return nil, fmt.Errorf("%w: reuse checkpoint %d outside confirmed range [%d,%d]",
			ErrOutOfRange, req.ReuseCheckpoint, task.FromCheckpoint, task.CopyWatermark)
	}
	replan := &Replan{
		RequestID:       req.RequestID,
		MigrationID:     task.ID,
		ShardID:         task.ShardID,
		Version:         task.Version,
		OldTarget:       task.Target,
		NewTarget:       req.NewTarget,
		Checkpoint:      task.CopyWatermark,
		ReuseCheckpoint: req.ReuseCheckpoint,
		Digest:          req.Digest,
		Reason:          req.Reason,
		State:           ReplanPending,
		CreatedAt:       m.now(),
	}
	task.PendingReplan = replan
	task.UpdatedAt = m.now()
	m.state.Replans[replan.RequestID] = replan
	m.auditLocked(task, EventReplanRequested,
		fmt.Sprintf("frozen version=%d checkpoint=%d old_target=%s new_target=%s reuse=%d reason=%s",
			replan.Version, replan.Checkpoint, replan.OldTarget, replan.NewTarget,
			replan.ReuseCheckpoint, replan.Reason))
	if err := m.store.Save(m.state); err != nil {
		return nil, err
	}
	dup := *replan
	return &dup, nil
}

// ConfirmReplan 确认重新规划：校验新目标对复用区间的摘要证明，生成新的
// 迁移版本（源归属与正式归属均不变），旧迁移进入 SUPERSEDED 终态，旧目标
// 停止领取。摘要不一致时拒绝，调用方需以更低（可证明一致）的检查点重新
// 申请，无法证明一致的部分将从安全位置重新复制。
func (m *Manager) ConfirmReplan(migrationID, proofDigest string) (*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.state.Tasks[migrationID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, migrationID)
	}
	replan := task.PendingReplan
	if replan == nil {
		return nil, fmt.Errorf("%w: %s", ErrNoPendingReplan, migrationID)
	}
	if task.Phase.Terminal() {
		return nil, fmt.Errorf("%w: %s is %s", ErrMigrationClosed, task.ID, task.Phase)
	}
	if task.Phase != PhaseCopying && task.Phase != PhaseCaughtUp {
		// 切换已先行签发：切换主线获胜，重新规划失效。
		return nil, fmt.Errorf("%w: %s is %s, cutover won the race", ErrInvalidPhase, task.ID, task.Phase)
	}
	if proofDigest != replan.Digest {
		m.auditLocked(task, EventReceiptRejected,
			fmt.Sprintf("replan proof digest mismatch for reuse checkpoint %d", replan.ReuseCheckpoint))
		_ = m.store.Save(m.state)
		return nil, fmt.Errorf("%w: reuse up to %d not proven", ErrDigestMismatch, replan.ReuseCheckpoint)
	}
	now := m.now()
	newTask := &Task{
		ID:              "mig-" + replan.RequestID,
		RequestID:       replan.RequestID,
		ShardID:         task.ShardID,
		Source:          task.Source,
		Target:          replan.NewTarget,
		Version:         task.Version + 1,
		FromCheckpoint:  replan.ReuseCheckpoint,
		ToCheckpoint:    task.ToCheckpoint,
		Phase:           PhaseCopying,
		CopyWatermark:   replan.ReuseCheckpoint,
		TargetWatermark: replan.ReuseCheckpoint,
		ReplanOf:        task.ID,
		ReuseCheckpoint: replan.ReuseCheckpoint,
		ReuseDigest:     replan.Digest,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if newTask.CopyWatermark >= newTask.ToCheckpoint {
		newTask.Phase = PhaseCaughtUp
	}
	task.Phase = PhaseSuperseded
	task.LeaseToken = ""
	task.PendingReplan = nil
	task.ReplannedBy = newTask.ID
	task.UpdatedAt = now
	replan.State = ReplanConfirmed
	replan.NewMigrationID = newTask.ID
	m.state.Tasks[newTask.ID] = newTask
	m.state.Requests[replan.RequestID] = newTask.ID
	m.auditLocked(task, EventMigrationSuperseded,
		fmt.Sprintf("superseded by %s (version %d), old target %s stops claiming",
			newTask.ID, newTask.Version, task.Target))
	m.auditLocked(newTask, EventReplanConfirmed,
		fmt.Sprintf("version %d target=%s reusing verified prefix [%d,%d] digest=%s; source ownership unchanged",
			newTask.Version, newTask.Target, newTask.FromCheckpoint, replan.ReuseCheckpoint, replan.Digest))
	if err := m.store.Save(m.state); err != nil {
		return nil, err
	}
	dup := *newTask
	return &dup, nil
}

// AbortReplan 放弃待确认的重新规划（例如新目标准备失败）：保留原迁移
// 及已确认进度，不会留下两个都可切换的目标。
func (m *Manager) AbortReplan(migrationID, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.state.Tasks[migrationID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, migrationID)
	}
	replan := task.PendingReplan
	if replan == nil {
		return fmt.Errorf("%w: %s", ErrNoPendingReplan, migrationID)
	}
	if task.Phase.Terminal() {
		return fmt.Errorf("%w: %s is %s", ErrMigrationClosed, task.ID, task.Phase)
	}
	task.PendingReplan = nil
	task.UpdatedAt = m.now()
	replan.State = ReplanAborted
	m.auditLocked(task, EventReplanAborted,
		fmt.Sprintf("replan %s aborted: %s; original migration and confirmed progress kept",
			replan.RequestID, reason))
	return m.store.Save(m.state)
}

// Replan 按重新规划号返回记录视图。
func (m *Manager) Replan(requestID string) (Replan, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	replan, ok := m.state.Replans[requestID]
	if !ok {
		return Replan{}, false
	}
	return *replan, true
}

// Status 返回分片当前归属与最近一次迁移的完整视图（含审计历史）。
func (m *Manager) Status(shardID string) (Status, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.statusLocked(shardID)
}

func (m *Manager) statusLocked(shardID string) (Status, bool) {
	owner, known := m.state.Owners[shardID]
	st := Status{ShardID: shardID, Owner: owner}
	var latest *Task
	for _, t := range m.state.Tasks {
		if t.ShardID != shardID {
			continue
		}
		if latest == nil || t.CreatedAt.After(latest.CreatedAt) ||
			(t.CreatedAt.Equal(latest.CreatedAt) && t.ID > latest.ID) {
			latest = t
		}
	}
	if latest != nil {
		m.fillTaskViewLocked(&st, latest)
	}
	for _, e := range m.state.Audit {
		if e.ShardID == shardID {
			st.Audit = append(st.Audit, e)
		}
	}
	return st, known || latest != nil
}

// fillTaskViewLocked 将任务及其版本关系、进度复用依据填入查询视图。
func (m *Manager) fillTaskViewLocked(st *Status, task *Task) {
	st.HasMigration = true
	st.MigrationID = task.ID
	st.Phase = task.Phase
	st.Version = task.Version
	st.Source = task.Source
	st.Target = task.Target
	st.FromCheckpoint = task.FromCheckpoint
	st.ToCheckpoint = task.ToCheckpoint
	st.CopyWatermark = task.CopyWatermark
	st.TargetWatermark = task.TargetWatermark
	st.LeaseToken = task.LeaseToken
	st.Attempts = task.Attempts
	st.FailureReason = task.FailureReason
	st.ReplanOf = task.ReplanOf
	st.ReuseCheckpoint = task.ReuseCheckpoint
	st.ReuseDigest = task.ReuseDigest
	if task.PendingReplan != nil {
		replan := *task.PendingReplan
		st.PendingReplan = &replan
	}
	st.ReplannedBy = task.ReplannedBy
	if st.ReplannedBy == "" {
		for _, other := range m.state.Tasks {
			if other.ReplanOf == task.ID {
				st.ReplannedBy = other.ID
				break
			}
		}
	}
}

// Migration 按 ID 返回该迁移任务的查询视图（含版本关系与复用依据）。
func (m *Manager) Migration(migrationID string) (Status, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.state.Tasks[migrationID]
	if !ok {
		return Status{}, false
	}
	st := Status{ShardID: task.ShardID, Owner: m.state.Owners[task.ShardID]}
	m.fillTaskViewLocked(&st, task)
	for _, e := range m.state.Audit {
		if e.ShardID == task.ShardID {
			st.Audit = append(st.Audit, e)
		}
	}
	return st, true
}

// Owner 返回分片当前正式归属。
func (m *Manager) Owner(shardID string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	owner, ok := m.state.Owners[shardID]
	return owner, ok
}

func (m *Manager) auditLocked(t *Task, event, detail string) {
	m.state.Audit = append(m.state.Audit, AuditEntry{
		Time:        m.now(),
		ShardID:     t.ShardID,
		MigrationID: t.ID,
		Event:       event,
		Detail:      detail,
	})
}
