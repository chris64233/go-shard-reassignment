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
	if r.Digest != "" {
		if task.Digests == nil {
			task.Digests = map[uint64]string{}
		}
		task.Digests[r.Checkpoint] = r.Digest
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

// RequestReplan 在迁移途中申请重新规划目标节点。只有迁移尚未切换正式
// 归属（复制中或追平待切换）且原目标明确不可继续（已下线）时才能申请。
// 申请会冻结原迁移版本、已确认检查点及摘要、原目标、候选新目标与失败
// 原因。相同重新规划号和内容返回原结果；检查点、候选目标或迁移版本
// 变化返回 ErrConflict。
func (m *Manager) RequestReplan(req ReplanRequest) (*Replan, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req.RequestID == "" || req.MigrationID == "" || req.Reason == "" || len(req.Candidates) == 0 {
		return nil, fmt.Errorf("%w: request_id/migration/reason/candidates are required", ErrInvalidRequest)
	}
	task, ok := m.state.Tasks[req.MigrationID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, req.MigrationID)
	}
	for _, c := range req.Candidates {
		if c == "" || c == task.Source {
			return nil, fmt.Errorf("%w: invalid candidate %q", ErrInvalidRequest, c)
		}
	}
	if id, ok := m.state.ReplanRequests[req.RequestID]; ok {
		existing := m.state.Replans[id]
		if !existing.sameParams(req, task) {
			return nil, fmt.Errorf("%w: replan %s reused with different parameters", ErrConflict, req.RequestID)
		}
		dup := *existing
		return &dup, nil
	}
	if task.Phase != PhaseCopying && task.Phase != PhaseCaughtUp {
		return nil, fmt.Errorf("%w: %s is %s, ownership already switching or closed",
			ErrInvalidPhase, task.ID, task.Phase)
	}
	if m.nodeUp(task.Target) {
		return nil, fmt.Errorf("%w: target %s is still available, replan not allowed",
			ErrInvalidPhase, task.Target)
	}
	for _, r := range m.state.Replans {
		if r.MigrationID == task.ID && r.NewMigrationID == "" {
			return nil, fmt.Errorf("%w: migration %s already has open replan %s",
				ErrConflict, task.ID, r.ID)
		}
	}
	frozenDigests := map[uint64]string{}
	for cp, d := range task.Digests {
		frozenDigests[cp] = d
	}
	replan := &Replan{
		ID:              "replan-" + req.RequestID,
		RequestID:       req.RequestID,
		MigrationID:     task.ID,
		ShardID:         task.ShardID,
		FrozenVersion:   task.Version,
		FrozenWatermark: task.CopyWatermark,
		FrozenDigests:   frozenDigests,
		OriginalTarget:  task.Target,
		Candidates:      append([]string(nil), req.Candidates...),
		Reason:          req.Reason,
		CreatedAt:       m.now(),
	}
	m.state.Replans[replan.ID] = replan
	m.state.ReplanRequests[req.RequestID] = replan.ID
	m.auditLocked(task, EventReplanRequested,
		fmt.Sprintf("replan %s frozen version=%d watermark=%d target=%s candidates=%v reason=%q",
			replan.ID, task.Version, task.CopyWatermark, task.Target, req.Candidates, req.Reason))
	if err := m.store.Save(m.state); err != nil {
		return nil, err
	}
	dup := *replan
	return &dup, nil
}

// ConfirmReplan 确认重新规划并生成新的迁移版本。新目标必须来自冻结的
// 候选集合且在线；可复用进度由可验证的检查点和摘要决定：仅当新目标
// 提交的 ReuseProof 与冻结的已确认检查点摘要一致时才复用到该检查点，
// 否则从安全位置（冻结范围起点）重新复制，绝不直接采信原目标上报的
// 最高数字。旧迁移在同一持久化事务内被取代（SUPERSEDED），源归属保持
// 不变，旧目标停止领取；旧目标迟到的复制或切换回执只能进入历史。
// 相同确认重复提交返回原结果，内容变化返回 ErrConflict。
func (m *Manager) ConfirmReplan(replanID, newTarget string, proof ReuseProof) (*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	replan, ok := m.state.Replans[replanID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, replanID)
	}
	if replan.NewMigrationID != "" {
		created := m.state.Tasks[replan.NewMigrationID]
		if created.Target != newTarget || created.Reuse == nil ||
			created.Reuse.Proof != proof {
			return nil, fmt.Errorf("%w: replan %s already confirmed with different parameters",
				ErrConflict, replanID)
		}
		dup := *created
		return &dup, nil
	}
	task := m.state.Tasks[replan.MigrationID]
	if task.Phase == PhaseCompleted {
		return nil, fmt.Errorf("%w: %s already completed, replan closed", ErrConflict, task.ID)
	}
	if task.Phase != PhaseCopying && task.Phase != PhaseCaughtUp {
		return nil, fmt.Errorf("%w: %s is %s", ErrInvalidPhase, task.ID, task.Phase)
	}
	candidate := false
	for _, c := range replan.Candidates {
		if c == newTarget {
			candidate = true
			break
		}
	}
	if !candidate {
		return nil, fmt.Errorf("%w: %s is not a frozen candidate", ErrConflict, newTarget)
	}
	if !m.nodeUp(newTarget) {
		return nil, fmt.Errorf("%w: %s is down", ErrNodeUnavailable, newTarget)
	}
	// 进度复用判定：仅采信与冻结摘要一致的已确认检查点。
	reusedFrom := task.FromCheckpoint
	verified := false
	if proof.Checkpoint > task.FromCheckpoint && proof.Checkpoint <= replan.FrozenWatermark &&
		proof.Checkpoint <= task.ToCheckpoint && proof.Digest != "" &&
		replan.FrozenDigests[proof.Checkpoint] == proof.Digest {
		reusedFrom = proof.Checkpoint
		verified = true
	}
	now := m.now()
	task.Phase = PhaseSuperseded
	task.LeaseToken = ""
	task.UpdatedAt = now
	m.auditLocked(task, EventMigrationSuperseded,
		fmt.Sprintf("superseded by replan %s; confirmed progress up to %d preserved",
			replan.ID, task.CopyWatermark))
	newTask := &Task{
		ID:              "mig-" + replan.RequestID,
		RequestID:       replan.RequestID,
		ShardID:         task.ShardID,
		Source:          task.Source,
		Target:          newTarget,
		Version:         task.Version + 1,
		FromCheckpoint:  reusedFrom,
		ToCheckpoint:    task.ToCheckpoint,
		Phase:           PhaseCopying,
		CopyWatermark:   reusedFrom,
		TargetWatermark: reusedFrom,
		ReplanOf:        task.ID,
		ReplanRequestID: replan.RequestID,
		Reuse: &ReuseBasis{
			Proof:      proof,
			Verified:   verified,
			ReusedFrom: reusedFrom,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	if reusedFrom >= newTask.ToCheckpoint {
		newTask.Phase = PhaseCaughtUp
	}
	m.state.Tasks[newTask.ID] = newTask
	replan.NewMigrationID = newTask.ID
	m.auditLocked(newTask, EventReplanConfirmed,
		fmt.Sprintf("replan %s confirmed: version=%d target=%s reused_from=%d verified=%t",
			replan.ID, newTask.Version, newTarget, reusedFrom, verified))
	if err := m.store.Save(m.state); err != nil {
		return nil, err
	}
	dup := *newTask
	return &dup, nil
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
		st.HasMigration = true
		st.MigrationID = latest.ID
		st.Phase = latest.Phase
		st.Version = latest.Version
		st.Source = latest.Source
		st.Target = latest.Target
		st.FromCheckpoint = latest.FromCheckpoint
		st.ToCheckpoint = latest.ToCheckpoint
		st.CopyWatermark = latest.CopyWatermark
		st.TargetWatermark = latest.TargetWatermark
		st.LeaseToken = latest.LeaseToken
		st.Attempts = latest.Attempts
		st.FailureReason = latest.FailureReason
		st.PreviousMigrationID = latest.ReplanOf
		if latest.Reuse != nil {
			reuse := *latest.Reuse
			st.Reuse = &reuse
		}
		// 最终目标：迁移链上最新一条未被取代的迁移的目标。
		head := latest
		for head.Phase == PhaseSuperseded {
			next, ok := m.successorLocked(head.ID)
			if !ok {
				break
			}
			head = next
		}
		st.FinalTarget = head.Target
	}
	for _, e := range m.state.Audit {
		if e.ShardID == shardID {
			st.Audit = append(st.Audit, e)
		}
	}
	return st, known || latest != nil
}

// successorLocked 返回取代给定迁移的新版本迁移（若存在）。
func (m *Manager) successorLocked(migrationID string) (*Task, bool) {
	for _, t := range m.state.Tasks {
		if t.ReplanOf == migrationID {
			return t, true
		}
	}
	return nil, false
}

// Migration 按 ID 返回迁移任务视图。
func (m *Manager) Migration(migrationID string) (Status, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.state.Tasks[migrationID]
	if !ok {
		return Status{}, false
	}
	st, _ := m.statusLocked(task.ShardID)
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
