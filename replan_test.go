package goshardreassignment

import (
	"errors"
	"path/filepath"
	"testing"
)

// 让原目标失联并推进到指定确认水位，返回迁移任务。
func setupReplanCandidate(t *testing.T, m *Manager, confirmed uint64) *Task {
	t.Helper()
	task := mustStart(t, m, startReq())
	if confirmed > task.FromCheckpoint {
		if err := m.SubmitReceipt(receipt(task, confirmed)); err != nil {
			t.Fatalf("receipt %d: %v", confirmed, err)
		}
	}
	if err := m.SetNodeDown(task.Target); err != nil {
		t.Fatal(err)
	}
	return task
}

func replanReq(task *Task) ReplanRequest {
	return ReplanRequest{
		RequestID:       "replan-1",
		MigrationID:     task.ID,
		NewTarget:       "node-c",
		Reason:          "node-b disk failure",
		ReuseCheckpoint: 150,
		Digest:          "sha256:prefix-150",
	}
}

func TestReplanRequestFreezesAndIdempotency(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := setupReplanCandidate(t, m, 150)

	rp, err := m.RequestReplan(replanReq(task))
	if err != nil {
		t.Fatalf("RequestReplan: %v", err)
	}
	if rp.Version != task.Version || rp.OldTarget != "node-b" || rp.NewTarget != "node-c" {
		t.Fatalf("replan must freeze version/targets: %+v", rp)
	}
	if rp.Checkpoint != 150 || rp.ReuseCheckpoint != 150 || rp.State != ReplanPending {
		t.Fatalf("replan must freeze confirmed checkpoint and reuse basis: %+v", rp)
	}

	// 相同重新规划号与内容：返回原结果。
	again, err := m.RequestReplan(replanReq(task))
	if err != nil {
		t.Fatalf("idempotent replan: %v", err)
	}
	if again.RequestID != rp.RequestID || again.CreatedAt != rp.CreatedAt {
		t.Fatalf("same replan id should return original record: %+v", again)
	}

	// 检查点、候选目标、摘要变化：冲突。
	changed := replanReq(task)
	changed.ReuseCheckpoint = 140
	if _, err := m.RequestReplan(changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed checkpoint should conflict, got %v", err)
	}
	changed = replanReq(task)
	changed.NewTarget = "node-d"
	if _, err := m.RequestReplan(changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed candidate should conflict, got %v", err)
	}
	changed = replanReq(task)
	changed.Digest = "sha256:other"
	if _, err := m.RequestReplan(changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed digest should conflict, got %v", err)
	}

	// 同一迁移已有待确认重新规划：新的规划号冲突。
	other := replanReq(task)
	other.RequestID = "replan-2"
	if _, err := m.RequestReplan(other); !errors.Is(err, ErrConflict) {
		t.Fatalf("second pending replan should conflict, got %v", err)
	}
}

func TestReplanPreconditions(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := mustStart(t, m, startReq())

	// 原目标仍在线：不允许重新规划。
	if _, err := m.RequestReplan(replanReq(task)); !errors.Is(err, ErrTargetViable) {
		t.Fatalf("replan with reachable target should fail, got %v", err)
	}
	if err := m.SetNodeDown(task.Target); err != nil {
		t.Fatal(err)
	}

	// 复用检查点不能超过已确认水位（当前只确认到 100）。
	req := replanReq(task)
	if _, err := m.RequestReplan(req); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("reuse beyond confirmed watermark should fail, got %v", err)
	}

	// 已切换正式归属后不允许重新规划。
	if err := m.SetNodeUp(task.Target); err != nil {
		t.Fatal(err)
	}
	if err := m.SubmitReceipt(receipt(task, 200)); err != nil {
		t.Fatal(err)
	}
	lease, err := m.BeginCutover(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ConfirmCutover(task.ID, lease); err != nil {
		t.Fatal(err)
	}
	if err := m.SetNodeDown(task.Target); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RequestReplan(replanReq(task)); !errors.Is(err, ErrMigrationClosed) {
		t.Fatalf("replan after cutover should fail, got %v", err)
	}
}

func TestReplanConfirmGeneratesNewVersion(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := setupReplanCandidate(t, m, 180)

	req := replanReq(task)
	req.ReuseCheckpoint = 150 // 只复用可验证前缀，150~180 从安全位置重复制
	if _, err := m.RequestReplan(req); err != nil {
		t.Fatal(err)
	}
	next, err := m.ConfirmReplan(task.ID, "sha256:prefix-150")
	if err != nil {
		t.Fatalf("ConfirmReplan: %v", err)
	}

	if next.Version != task.Version+1 || next.ID == task.ID {
		t.Fatalf("replan must generate a new migration version: %+v", next)
	}
	if next.Source != task.Source || next.Target != "node-c" {
		t.Fatalf("source must stay, target must move to node-c: %+v", next)
	}
	if next.CopyWatermark != 150 || next.FromCheckpoint != 150 || next.ToCheckpoint != 200 {
		t.Fatalf("new version resumes from verified checkpoint 150: %+v", next)
	}
	if next.ReplanOf != task.ID || next.ReuseDigest != "sha256:prefix-150" {
		t.Fatalf("reuse basis must be recorded: %+v", next)
	}
	if owner, _ := m.Owner(task.ShardID); owner != "node-a" {
		t.Fatalf("ownership must remain node-a, got %s", owner)
	}

	// 旧目标迟到的复制回执：只进历史，不影响新版本。
	late := receipt(task, 200)
	if err := m.SubmitReceipt(late); !errors.Is(err, ErrMigrationClosed) {
		t.Fatalf("late receipt from old target should be rejected, got %v", err)
	}
	// 旧目标迟到的切换回执：只进历史，不改变正式归属。
	if err := m.ConfirmCutover(task.ID, "mig-req-1/attempt-1"); !errors.Is(err, ErrMigrationClosed) {
		t.Fatalf("late cutover confirm should be rejected, got %v", err)
	}
	if owner, _ := m.Owner(task.ShardID); owner != "node-a" {
		t.Fatalf("late receipts must not change ownership, got %s", owner)
	}
	st, _ := m.Status(task.ShardID)
	lateSeen := false
	for _, e := range st.Audit {
		if e.Event == EventLateReceipt || e.Event == EventLateConfirm {
			lateSeen = true
		}
	}
	if !lateSeen {
		t.Fatal("late receipts/confirms must be recorded to history")
	}

	// 新版本继续推进并完成切换；旧目标恢复不影响主线。
	if err := m.SetNodeUp("node-b"); err != nil {
		t.Fatal(err)
	}
	if err := m.SubmitReceipt(receipt(next, 200)); err != nil {
		t.Fatalf("receipt on new version: %v", err)
	}
	lease, err := m.BeginCutover(next.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ConfirmCutover(next.ID, lease); err != nil {
		t.Fatal(err)
	}
	if owner, _ := m.Owner(task.ShardID); owner != "node-c" {
		t.Fatalf("final owner should be node-c, got %s", owner)
	}
}

func TestReplanDigestMismatchAndAbort(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := setupReplanCandidate(t, m, 150)
	if _, err := m.RequestReplan(replanReq(task)); err != nil {
		t.Fatal(err)
	}

	// 新目标无法证明复用区间一致：拒绝，原迁移与进度保留。
	if _, err := m.ConfirmReplan(task.ID, "sha256:wrong"); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("digest mismatch should fail, got %v", err)
	}
	st, _ := m.Status(task.ShardID)
	if st.Phase != PhaseCopying || st.CopyWatermark != 150 || st.Target != "node-b" {
		t.Fatalf("original migration must be preserved: %+v", st)
	}

	// 新目标准备失败：放弃重新规划，不能留下两个可切换目标。
	if err := m.AbortReplan(task.ID, "node-c capacity insufficient"); err != nil {
		t.Fatal(err)
	}
	st, _ = m.Status(task.ShardID)
	if st.PendingReplan != nil || st.Phase != PhaseCopying || st.CopyWatermark != 150 {
		t.Fatalf("abort must keep original migration and progress: %+v", st)
	}
	if v, ok := m.Migration(task.ID); !ok || v.Phase.Terminal() {
		t.Fatalf("original migration must remain the single active mainline: %+v", v)
	}

	// 放弃后可用新规划号重新申请并确认。
	req := replanReq(task)
	req.RequestID = "replan-2"
	if _, err := m.RequestReplan(req); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ConfirmReplan(task.ID, req.Digest); err != nil {
		t.Fatal(err)
	}
}

func TestReplanRaceWithCutoverAndRecovery(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := setupReplanCandidate(t, m, 200)

	// 切换中目标失联：安全回退，此时可申请重新规划。
	if err := m.SetNodeUp(task.Target); err != nil {
		t.Fatal(err)
	}
	if _, err := m.BeginCutover(task.ID); err != nil {
		t.Fatal(err)
	}
	if err := m.SetNodeDown(task.Target); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RequestReplan(replanReq(task)); err != nil {
		t.Fatalf("replan after cutover rollback: %v", err)
	}

	// 原目标恢复并抢先完成切换：重新规划确认必须失败，只有一条主线。
	if err := m.SetNodeUp(task.Target); err != nil {
		t.Fatal(err)
	}
	lease, err := m.BeginCutover(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ConfirmCutover(task.ID, lease); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ConfirmReplan(task.ID, "sha256:prefix-150"); !errors.Is(err, ErrMigrationClosed) {
		t.Fatalf("replan confirm after cutover should fail, got %v", err)
	}
	if owner, _ := m.Owner(task.ShardID); owner != "node-b" {
		t.Fatalf("cutover mainline should win, got %s", owner)
	}
}

func TestReplanConfirmWinsBeforeCutover(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := setupReplanCandidate(t, m, 200)
	if _, err := m.RequestReplan(replanReq(task)); err != nil {
		t.Fatal(err)
	}
	next, err := m.ConfirmReplan(task.ID, "sha256:prefix-150")
	if err != nil {
		t.Fatal(err)
	}
	// 重新规划先生效：旧迁移上的切换签发被拒绝，新目标成为唯一主线。
	if err := m.SetNodeUp("node-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.BeginCutover(task.ID); !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("cutover on superseded migration should fail, got %v", err)
	}
	if err := m.SubmitReceipt(receipt(next, 200)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.BeginCutover(next.ID); err != nil {
		t.Fatalf("new version should be the only switchable mainline: %v", err)
	}
}

func TestReplanStatusAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	store := NewFileStore(path)
	m := newManager(t, store)
	task := setupReplanCandidate(t, m, 150)

	// 待确认的重新规划在重启后保留。
	if _, err := m.RequestReplan(replanReq(task)); err != nil {
		t.Fatal(err)
	}
	m2 := newManager(t, store)
	st, _ := m2.Status(task.ShardID)
	if st.PendingReplan == nil || st.PendingReplan.RequestID != "replan-1" {
		t.Fatalf("pending replan must survive restart: %+v", st)
	}

	next, err := m2.ConfirmReplan(task.ID, "sha256:prefix-150")
	if err != nil {
		t.Fatal(err)
	}

	// 查询展示版本关系、进度复用依据与最终目标。
	st, _ = m2.Status(task.ShardID)
	if st.MigrationID != next.ID || st.ReplanOf != task.ID || st.Target != "node-c" {
		t.Fatalf("status must show version relation and final target: %+v", st)
	}
	if st.ReuseCheckpoint != 150 || st.ReuseDigest != "sha256:prefix-150" {
		t.Fatalf("status must show reuse basis: %+v", st)
	}
	old, ok := m2.Migration(task.ID)
	if !ok || old.Phase != PhaseSuperseded || old.ReplannedBy != next.ID {
		t.Fatalf("old version must show superseded and successor: %+v", old)
	}
	if old.CopyWatermark != 150 {
		t.Fatalf("old version confirmed progress must remain queryable: %+v", old)
	}

	// 重启后 SUPERSEDED 终态保持，迟到回执仍只进历史。
	m3 := newManager(t, store)
	old, _ = m3.Migration(task.ID)
	if old.Phase != PhaseSuperseded {
		t.Fatalf("superseded must persist across restart, got %s", old.Phase)
	}
	if err := m3.SubmitReceipt(receipt(task, 160)); !errors.Is(err, ErrMigrationClosed) {
		t.Fatalf("late receipt after restart should be rejected, got %v", err)
	}
	if owner, _ := m3.Owner(task.ShardID); owner != "node-a" {
		t.Fatalf("ownership must remain node-a, got %s", owner)
	}
}
