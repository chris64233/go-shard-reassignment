package goshardreassignment

import (
	"errors"
	"path/filepath"
	"testing"
)

func newManager(t *testing.T, store Store) *Manager {
	t.Helper()
	m, err := NewManager(store)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

func startReq() StartRequest {
	return StartRequest{
		RequestID:      "req-1",
		ShardID:        "shard-1",
		Source:         "node-a",
		Target:         "node-b",
		Version:        7,
		FromCheckpoint: 100,
		ToCheckpoint:   200,
	}
}

func mustStart(t *testing.T, m *Manager, req StartRequest) *Task {
	t.Helper()
	task, err := m.StartMigration(req)
	if err != nil {
		t.Fatalf("StartMigration: %v", err)
	}
	return task
}

func receipt(task *Task, checkpoint uint64) Receipt {
	return Receipt{
		MigrationID: task.ID,
		ShardID:     task.ShardID,
		NodeID:      task.Target,
		Version:     task.Version,
		Checkpoint:  checkpoint,
	}
}

func TestIdempotentStartAndConflicts(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := mustStart(t, m, startReq())

	again := mustStart(t, m, startReq())
	if again.ID != task.ID {
		t.Fatalf("duplicate submit should return original task %s, got %s", task.ID, again.ID)
	}

	changed := startReq()
	changed.Version = 8
	if _, err := m.StartMigration(changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed version with same request id should conflict, got %v", err)
	}
	changed = startReq()
	changed.Target = "node-c"
	if _, err := m.StartMigration(changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed target with same request id should conflict, got %v", err)
	}
	changed = startReq()
	changed.Source = "node-c"
	if _, err := m.StartMigration(changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed source with same request id should conflict, got %v", err)
	}

	other := startReq()
	other.RequestID = "req-2"
	if _, err := m.StartMigration(other); !errors.Is(err, ErrConflict) {
		t.Fatalf("second active migration on same shard should conflict, got %v", err)
	}

	if err := m.RegisterShard("shard-2", "node-a"); err != nil {
		t.Fatal(err)
	}
	bad := startReq()
	bad.RequestID = "req-3"
	bad.ShardID = "shard-2"
	bad.Source = "node-c" // 与登记归属不一致
	if _, err := m.StartMigration(bad); !errors.Is(err, ErrConflict) {
		t.Fatalf("migration from non-owner should conflict, got %v", err)
	}
}

func TestReceiptsMonotonicAndCatchUp(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := mustStart(t, m, startReq())

	if err := m.SubmitReceipt(receipt(task, 150)); err != nil {
		t.Fatalf("receipt 150: %v", err)
	}
	if err := m.SubmitReceipt(receipt(task, 150)); err != nil {
		t.Fatalf("duplicate receipt should be idempotent: %v", err)
	}
	if err := m.SubmitReceipt(receipt(task, 140)); !errors.Is(err, ErrWatermarkRegression) {
		t.Fatalf("regressed checkpoint should be rejected, got %v", err)
	}
	stale := receipt(task, 160)
	stale.Version = task.Version - 1
	if err := m.SubmitReceipt(stale); !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("stale version should be rejected, got %v", err)
	}
	if err := m.SubmitReceipt(receipt(task, 250)); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("checkpoint beyond frozen range should be rejected, got %v", err)
	}

	st, _ := m.Status(task.ShardID)
	if st.CopyWatermark != 150 || st.Phase != PhaseCopying {
		t.Fatalf("unexpected status after rejections: %+v", st)
	}

	if err := m.SubmitReceipt(receipt(task, 200)); err != nil {
		t.Fatalf("receipt 200: %v", err)
	}
	st, _ = m.Status(task.ShardID)
	if st.Phase != PhaseCaughtUp {
		t.Fatalf("expected CAUGHT_UP after catch-up, got %s", st.Phase)
	}
	if owner, _ := m.Owner(task.ShardID); owner != "node-a" {
		t.Fatalf("ownership must not change before cutover, got %s", owner)
	}
}

func TestNodeFailureBlocksProgressAndCutover(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := mustStart(t, m, startReq())

	if err := m.SetNodeDown("node-b"); err != nil {
		t.Fatal(err)
	}
	if err := m.SubmitReceipt(receipt(task, 200)); !errors.Is(err, ErrNodeUnavailable) {
		t.Fatalf("receipt while target down should fail, got %v", err)
	}
	st, _ := m.Status(task.ShardID)
	if st.CopyWatermark != 100 || st.Phase != PhaseCopying {
		t.Fatalf("partial copy must not be reported complete while node down: %+v", st)
	}

	if err := m.SetNodeUp("node-b"); err != nil {
		t.Fatal(err)
	}
	if err := m.SubmitReceipt(receipt(task, 200)); err != nil {
		t.Fatalf("receipt after node recovery: %v", err)
	}

	if err := m.SetNodeDown("node-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.BeginCutover(task.ID); !errors.Is(err, ErrNodeUnavailable) {
		t.Fatalf("cutover while target down should fail, got %v", err)
	}
	if err := m.SetNodeUp("node-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.BeginCutover(task.ID); err != nil {
		t.Fatalf("cutover after recovery: %v", err)
	}
}

func TestCutoverRaceSingleOutcome(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := mustStart(t, m, startReq())
	if err := m.SubmitReceipt(receipt(task, 200)); err != nil {
		t.Fatal(err)
	}

	lease1, err := m.BeginCutover(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	// 目标节点在切换中途失联：本次尝试被安全回退。
	if err := m.SetNodeDown("node-b"); err != nil {
		t.Fatal(err)
	}
	if err := m.SetNodeUp("node-b"); err != nil {
		t.Fatal(err)
	}
	lease2, err := m.BeginCutover(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if lease1 == lease2 {
		t.Fatal("each cutover attempt must issue a distinct lease")
	}
	if err := m.ConfirmCutover(task.ID, lease1); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale lease confirm should fail, got %v", err)
	}
	// 并发失败竞争：确认先落盘者获胜。
	if err := m.ConfirmCutover(task.ID, lease2); err != nil {
		t.Fatalf("confirm with valid lease: %v", err)
	}
	if err := m.FailMigration(task.ID, "too late"); !errors.Is(err, ErrConflict) {
		t.Fatalf("failing a completed migration should conflict, got %v", err)
	}
	if err := m.ConfirmCutover(task.ID, lease2); err != nil {
		t.Fatalf("re-confirm with same lease should be idempotent: %v", err)
	}
	if owner, _ := m.Owner(task.ShardID); owner != "node-b" {
		t.Fatalf("ownership should switch exactly once to node-b, got %s", owner)
	}
	st, _ := m.Status(task.ShardID)
	if st.Phase != PhaseCompleted || st.Attempts != 2 {
		t.Fatalf("unexpected final status: %+v", st)
	}
}

func TestCutoverAbortAndRetry(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := mustStart(t, m, startReq())
	if err := m.SubmitReceipt(receipt(task, 200)); err != nil {
		t.Fatal(err)
	}
	lease, err := m.BeginCutover(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.AbortCutover(task.ID, "bad-lease", "timeout"); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("abort with wrong lease should fail, got %v", err)
	}
	if err := m.AbortCutover(task.ID, lease, "confirm timeout"); err != nil {
		t.Fatalf("abort with valid lease: %v", err)
	}
	st, _ := m.Status(task.ShardID)
	if st.Phase != PhaseCaughtUp || st.CopyWatermark != 200 {
		t.Fatalf("abort should roll back to CAUGHT_UP with watermark kept: %+v", st)
	}
	lease2, err := m.BeginCutover(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ConfirmCutover(task.ID, lease2); err != nil {
		t.Fatal(err)
	}
	if owner, _ := m.Owner(task.ShardID); owner != "node-b" {
		t.Fatalf("ownership should be node-b after retried cutover, got %s", owner)
	}
}

func TestRestartRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	store := NewFileStore(path)

	m := newManager(t, store)
	task := mustStart(t, m, startReq())
	if err := m.SubmitReceipt(receipt(task, 200)); err != nil {
		t.Fatal(err)
	}
	lease, err := m.BeginCutover(task.ID)
	if err != nil {
		t.Fatal(err)
	}

	// 模拟进程重启：切换中崩溃，恢复后不得重复切换或丢失检查点。
	m2 := newManager(t, store)
	st, _ := m2.Status(task.ShardID)
	if st.Phase != PhaseCaughtUp {
		t.Fatalf("recovered task should be CAUGHT_UP, got %s", st.Phase)
	}
	if st.CopyWatermark != 200 {
		t.Fatalf("checkpoint lost after restart: %d", st.CopyWatermark)
	}
	if owner, _ := m2.Owner(task.ShardID); owner != "node-a" {
		t.Fatalf("ownership must remain node-a after unconfirmed cutover, got %s", owner)
	}
	if err := m2.ConfirmCutover(task.ID, lease); !errors.Is(err, ErrLeaseMismatch) && !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("pre-crash lease must be invalidated, got %v", err)
	}

	lease2, err := m2.BeginCutover(task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.ConfirmCutover(task.ID, lease2); err != nil {
		t.Fatal(err)
	}

	// 再次重启：已完成任务保持完成，归属不重复切换。
	m3 := newManager(t, store)
	st, _ = m3.Status(task.ShardID)
	if st.Phase != PhaseCompleted || st.Owner != "node-b" {
		t.Fatalf("unexpected state after second restart: %+v", st)
	}
	if err := m3.ConfirmCutover(task.ID, lease2); err != nil {
		t.Fatalf("confirm after restart with same lease should be idempotent: %v", err)
	}
	// 重启后同一 RequestID 重复提交仍返回原任务。
	dup, err := m3.StartMigration(startReq())
	if err != nil {
		t.Fatalf("duplicate submit after restart: %v", err)
	}
	if dup.ID != task.ID {
		t.Fatalf("duplicate submit after restart should return %s, got %s", task.ID, dup.ID)
	}
}

func TestLateReceiptAfterCompletion(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := mustStart(t, m, startReq())
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

	late := receipt(task, 180)
	if err := m.SubmitReceipt(late); !errors.Is(err, ErrMigrationClosed) {
		t.Fatalf("late receipt should be rejected, got %v", err)
	}
	st, _ := m.Status(task.ShardID)
	if st.CopyWatermark != 200 || st.Phase != PhaseCompleted {
		t.Fatalf("late receipt must not corrupt state: %+v", st)
	}
}

func TestFailureRollbackKeepsProgress(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := mustStart(t, m, startReq())
	if err := m.SubmitReceipt(receipt(task, 180)); err != nil {
		t.Fatal(err)
	}
	if err := m.FailMigration(task.ID, "target disk full"); err != nil {
		t.Fatal(err)
	}
	if err := m.FailMigration(task.ID, "target disk full"); err != nil {
		t.Fatalf("duplicate fail should be idempotent: %v", err)
	}

	st, _ := m.Status(task.ShardID)
	if st.Phase != PhaseFailed || st.Owner != "node-a" {
		t.Fatalf("failure should roll back to source: %+v", st)
	}
	if st.CopyWatermark != 180 {
		t.Fatalf("confirmed progress should remain queryable, got %d", st.CopyWatermark)
	}
	if st.FailureReason != "target disk full" {
		t.Fatalf("failure reason missing: %q", st.FailureReason)
	}
	if err := m.SubmitReceipt(receipt(task, 190)); !errors.Is(err, ErrMigrationClosed) {
		t.Fatalf("receipt after failure should be rejected, got %v", err)
	}

	// 失败是终态，同一分片可以发起新的迁移。
	next := startReq()
	next.RequestID = "req-2"
	next.Version = 8
	if _, err := m.StartMigration(next); err != nil {
		t.Fatalf("new migration after failure should be allowed: %v", err)
	}
}

func TestStatusQueryContents(t *testing.T) {
	m := newManager(t, NewMemoryStore())
	task := mustStart(t, m, startReq())
	if err := m.SubmitReceipt(receipt(task, 200)); err != nil {
		t.Fatal(err)
	}
	lease, err := m.BeginCutover(task.ID)
	if err != nil {
		t.Fatal(err)
	}

	st, ok := m.Status(task.ShardID)
	if !ok {
		t.Fatal("status not found")
	}
	if st.Owner != "node-a" || st.Phase != PhaseCuttingOver {
		t.Fatalf("unexpected status: %+v", st)
	}
	if st.CopyWatermark != 200 || st.TargetWatermark != 200 {
		t.Fatalf("watermarks missing: %+v", st)
	}
	if st.Version != 7 || st.LeaseToken != lease || st.Attempts != 1 {
		t.Fatalf("version/lease/attempt info missing: %+v", st)
	}
	events := map[string]bool{}
	for _, e := range st.Audit {
		events[e.Event] = true
	}
	for _, want := range []string{EventMigrationStarted, EventReceiptAccepted, EventCaughtUp, EventCutoverStarted} {
		if !events[want] {
			t.Fatalf("audit history missing event %s", want)
		}
	}

	if _, ok := m.Migration(task.ID); !ok {
		t.Fatal("migration lookup by id failed")
	}
	if _, ok := m.Migration("mig-nope"); ok {
		t.Fatal("unknown migration should not be found")
	}
}
