package goshardreassignment

import (
	"errors"
	"path/filepath"
	"testing"
)

func receiptWithDigest(task *Task, checkpoint uint64, digest string) Receipt {
	r := receipt(task, checkpoint)
	r.Digest = digest
	return r
}

// 准备一条目标已下线、带有已确认检查点摘要的迁移。
func setupReplanable(t *testing.T, m *Manager) *Task {
	t.Helper()
	task := mustStart(t, m, startReq())
	if err := m.SubmitReceipt(receiptWithDigest(task, 150, "digest-150")); err != nil {
		t.Fatalf("receipt 150: %v", err)
	}
	if err := m.SetNodeDown("node-b"); err != nil {
		t.Fatal(err)
	}
	return task
}

func replanReq(task *Task) ReplanRequest {
	return ReplanRequest{
		RequestID:   "rp-1",
		MigrationID: task.ID,
		Reason:      "node-b disk failure",
		Candidates:  []string{"node-c", "node-d"},
	}
}

func TestReplanRequestGating(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := mustStart(t, m, startReq())

	// 目标仍在线：不允许重新规划。
	if _, err := m.RequestReplan(replanReq(task)); !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("replan while target up should fail, got %v", err)
	}
	if err := m.SetNodeDown("node-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RequestReplan(ReplanRequest{}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("empty replan request should fail, got %v", err)
	}
	bad := replanReq(task)
	bad.Candidates = []string{"node-a"} // 候选不能是源节点
	if _, err := m.RequestReplan(bad); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("candidate equal to source should fail, got %v", err)
	}
	if _, err := m.RequestReplan(replanReq(task)); err != nil {
		t.Fatalf("replan with target down: %v", err)
	}

	// 切换确认后（正式归属已改写）不允许重新规划。
	m2 := newManager(t, NewMemoryStore())
	task2 := mustStart(t, m2, startReq())
	if err := m2.SubmitReceipt(receipt(task2, 200)); err != nil {
		t.Fatal(err)
	}
	lease, err := m2.BeginCutover(task2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.ConfirmCutover(task2.ID, lease); err != nil {
		t.Fatal(err)
	}
	if err := m2.SetNodeDown("node-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := m2.RequestReplan(replanReq(task2)); !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("replan after ownership switched should fail, got %v", err)
	}
}

func TestReplanIdempotencyAndConflict(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := setupReplanable(t, m)

	rp, err := m.RequestReplan(replanReq(task))
	if err != nil {
		t.Fatalf("RequestReplan: %v", err)
	}
	if rp.FrozenVersion != 7 || rp.FrozenWatermark != 150 || rp.OriginalTarget != "node-b" {
		t.Fatalf("replan should freeze version/watermark/target: %+v", rp)
	}
	if rp.FrozenDigests[150] != "digest-150" {
		t.Fatalf("replan should freeze confirmed digests: %+v", rp.FrozenDigests)
	}

	// 相同重新规划号和内容返回原结果。
	again, err := m.RequestReplan(replanReq(task))
	if err != nil {
		t.Fatalf("duplicate replan should be idempotent: %v", err)
	}
	if again.ID != rp.ID {
		t.Fatalf("duplicate replan should return original %s, got %s", rp.ID, again.ID)
	}

	// 候选目标变化返回冲突。
	changed := replanReq(task)
	changed.Candidates = []string{"node-e"}
	if _, err := m.RequestReplan(changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed candidates should conflict, got %v", err)
	}
	// 检查点变化（新回执确认后重放相同请求号）返回冲突。
	if err := m.SetNodeUp("node-b"); err != nil {
		t.Fatal(err)
	}
	if err := m.SubmitReceipt(receiptWithDigest(task, 160, "digest-160")); err != nil {
		t.Fatal(err)
	}
	if err := m.SetNodeDown("node-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RequestReplan(replanReq(task)); !errors.Is(err, ErrConflict) {
		t.Fatalf("checkpoint change with same replan id should conflict, got %v", err)
	}
	// 同一迁移存在未确认的重新规划时，新规划号冲突。
	other := replanReq(task)
	other.RequestID = "rp-2"
	if _, err := m.RequestReplan(other); !errors.Is(err, ErrConflict) {
		t.Fatalf("second open replan should conflict, got %v", err)
	}
}

func TestReplanConfirmReusesVerifiedProgress(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := setupReplanable(t, m)
	rp, err := m.RequestReplan(replanReq(task))
	if err != nil {
		t.Fatal(err)
	}

	// 非候选目标、候选目标不在线均被拒绝。
	if _, err := m.ConfirmReplan(rp.ID, "node-e", ReuseProof{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("non-candidate target should conflict, got %v", err)
	}
	if err := m.SetNodeDown("node-c"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ConfirmReplan(rp.ID, "node-c", ReuseProof{}); !errors.Is(err, ErrNodeUnavailable) {
		t.Fatalf("candidate down should fail, got %v", err)
	}
	if err := m.SetNodeUp("node-c"); err != nil {
		t.Fatal(err)
	}

	// 摘要可验证：复用到已确认检查点 150，而不是原目标上报的最高数字。
	nt, err := m.ConfirmReplan(rp.ID, "node-c", ReuseProof{Checkpoint: 150, Digest: "digest-150"})
	if err != nil {
		t.Fatalf("ConfirmReplan: %v", err)
	}
	if nt.Version != task.Version+1 || nt.Source != "node-a" || nt.Target != "node-c" {
		t.Fatalf("new version should keep source and bump version: %+v", nt)
	}
	if nt.CopyWatermark != 150 || nt.FromCheckpoint != 150 || nt.ToCheckpoint != 200 {
		t.Fatalf("verified reuse should resume at 150 within frozen range: %+v", nt)
	}
	if nt.ReplanOf != task.ID || nt.Reuse == nil || !nt.Reuse.Verified || nt.Reuse.ReusedFrom != 150 {
		t.Fatalf("reuse basis missing: %+v", nt)
	}

	// 源归属保持不变，旧迁移被取代。
	if owner, _ := m.Owner(task.ShardID); owner != "node-a" {
		t.Fatalf("ownership must stay on source, got %s", owner)
	}
	st, _ := m.Status(task.ShardID)
	if st.Phase != PhaseCopying || st.Version != 8 || st.Target != "node-c" {
		t.Fatalf("status should show new version: %+v", st)
	}
	if st.PreviousMigrationID != task.ID || st.FinalTarget != "node-c" {
		t.Fatalf("status should show version lineage and final target: %+v", st)
	}
	if st.Reuse == nil || !st.Reuse.Verified || st.Reuse.ReusedFrom != 150 {
		t.Fatalf("status should expose reuse basis: %+v", st.Reuse)
	}

	// 相同确认重复提交返回原结果；内容变化返回冲突。
	dup, err := m.ConfirmReplan(rp.ID, "node-c", ReuseProof{Checkpoint: 150, Digest: "digest-150"})
	if err != nil || dup.ID != nt.ID {
		t.Fatalf("duplicate confirm should be idempotent: %v %+v", err, dup)
	}
	if _, err := m.ConfirmReplan(rp.ID, "node-d", ReuseProof{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("confirm with different target should conflict, got %v", err)
	}
}

func TestReplanConfirmUnverifiableProofRestartsFromSafePoint(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := setupReplanable(t, m)
	rp, err := m.RequestReplan(replanReq(task))
	if err != nil {
		t.Fatal(err)
	}

	// 摘要无法证明一致：从安全位置（冻结范围起点）重新复制。
	nt, err := m.ConfirmReplan(rp.ID, "node-c", ReuseProof{Checkpoint: 150, Digest: "forged"})
	if err != nil {
		t.Fatal(err)
	}
	if nt.CopyWatermark != 100 || nt.FromCheckpoint != 100 {
		t.Fatalf("unverifiable proof must restart from safe point 100: %+v", nt)
	}
	if nt.Reuse.Verified {
		t.Fatal("forged digest must not be marked verified")
	}
}

func TestReplanLateReceiptsOnlyEnterHistory(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := setupReplanable(t, m)
	rp, err := m.RequestReplan(replanReq(task))
	if err != nil {
		t.Fatal(err)
	}
	nt, err := m.ConfirmReplan(rp.ID, "node-c", ReuseProof{Checkpoint: 150, Digest: "digest-150"})
	if err != nil {
		t.Fatal(err)
	}

	// 旧目标迟到的复制回执只能进入历史，不能推进新版本。
	if err := m.SubmitReceipt(receipt(task, 200)); !errors.Is(err, ErrMigrationClosed) {
		t.Fatalf("late receipt on superseded migration should fail, got %v", err)
	}
	// 旧目标迟到的切换确认不能改变正式归属。
	if err := m.ConfirmCutover(task.ID, "stale-lease"); !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("late cutover confirm should fail, got %v", err)
	}
	if owner, _ := m.Owner(task.ShardID); owner != "node-a" {
		t.Fatalf("late receipts must not change ownership, got %s", owner)
	}
	st, _ := m.Status(task.ShardID)
	if st.CopyWatermark != 150 || st.Version != 8 {
		t.Fatalf("late receipt must not advance new version: %+v", st)
	}

	// 新版本正常推进并完成切换，最终归属新目标。
	if err := m.SubmitReceipt(receipt(nt, 200)); err != nil {
		t.Fatal(err)
	}
	lease, err := m.BeginCutover(nt.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ConfirmCutover(nt.ID, lease); err != nil {
		t.Fatal(err)
	}
	if owner, _ := m.Owner(task.ShardID); owner != "node-c" {
		t.Fatalf("ownership should switch to node-c, got %s", owner)
	}
}

func TestReplanNewTargetFailureKeepsSingleMainline(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := setupReplanable(t, m)
	rp, err := m.RequestReplan(replanReq(task))
	if err != nil {
		t.Fatal(err)
	}
	nt, err := m.ConfirmReplan(rp.ID, "node-c", ReuseProof{Checkpoint: 150, Digest: "digest-150"})
	if err != nil {
		t.Fatal(err)
	}

	// 新目标准备失败：保留原迁移及已确认进度，不能留下两个可切换目标。
	if err := m.FailMigration(nt.ID, "node-c prepare failed"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.BeginCutover(task.ID); !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("superseded migration must not be cutover-able, got %v", err)
	}
	if _, err := m.BeginCutover(nt.ID); !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("failed migration must not be cutover-able, got %v", err)
	}
	if owner, _ := m.Owner(task.ShardID); owner != "node-a" {
		t.Fatalf("ownership must remain on source, got %s", owner)
	}
	// 原迁移的已确认进度保留可查。
	found := false
	st, _ := m.Status(task.ShardID)
	for _, e := range st.Audit {
		if e.Event == EventMigrationSuperseded {
			found = true
		}
	}
	if !found {
		t.Fatal("audit should record superseded event with preserved progress")
	}
}

func TestReplanSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	store := NewFileStore(path)

	m := newManager(t, store)
	task := setupReplanable(t, m)
	rp, err := m.RequestReplan(replanReq(task))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.ConfirmReplan(rp.ID, "node-c", ReuseProof{Checkpoint: 150, Digest: "digest-150"}); err != nil {
		t.Fatal(err)
	}

	// 重启后：版本关系、复用依据与最终目标仍然可查。
	m2 := newManager(t, store)
	st, ok := m2.Status(task.ShardID)
	if !ok || st.Version != 8 || st.Target != "node-c" || st.CopyWatermark != 150 {
		t.Fatalf("replanned migration should survive restart: %+v", st)
	}
	if st.PreviousMigrationID != task.ID || st.FinalTarget != "node-c" ||
		st.Reuse == nil || !st.Reuse.Verified {
		t.Fatalf("lineage and reuse basis should survive restart: %+v", st)
	}
	if owner, _ := m2.Owner(task.ShardID); owner != "node-a" {
		t.Fatalf("ownership should remain on source after restart, got %s", owner)
	}
}
