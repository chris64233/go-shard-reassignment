package goshardreassignment

import (
	"errors"
	"testing"
)

func newServiceWithTopology(t *testing.T) *Service {
	t.Helper()
	svc := NewService()
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		if _, err := svc.RegisterNode(id); err != nil {
			t.Fatalf("RegisterNode(%s): %v", id, err)
		}
	}
	if _, err := svc.CreateShard("shard-1", 1024, "node-a"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}
	return svc
}

func TestRegisterNodeRejectsDuplicates(t *testing.T) {
	svc := NewService()
	if _, err := svc.RegisterNode("node-a"); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if _, err := svc.RegisterNode("node-a"); !errors.Is(err, ErrNodeExists) {
		t.Fatalf("expected ErrNodeExists, got %v", err)
	}
}

func TestCreateShardValidation(t *testing.T) {
	svc := NewService()
	if _, err := svc.RegisterNode("node-a"); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}

	if _, err := svc.CreateShard("shard-1", 0, "node-a"); !errors.Is(err, ErrInvalidShardCapacity) {
		t.Fatalf("expected ErrInvalidShardCapacity, got %v", err)
	}
	if _, err := svc.CreateShard("shard-1", 10, "ghost"); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
	if _, err := svc.CreateShard("shard-1", 10, "node-a"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}
	if _, err := svc.CreateShard("shard-1", 20, "node-a"); !errors.Is(err, ErrShardExists) {
		t.Fatalf("expected ErrShardExists, got %v", err)
	}
}

func TestMigrationLifecycleKeepsSingleOwner(t *testing.T) {
	svc := newServiceWithTopology(t)

	mig, err := svc.SubmitMigration(MigrationRequest{
		RequestKey:   "req-1",
		ShardID:      "shard-1",
		SourceNodeID: "node-a",
		TargetNodeID: "node-b",
		Reason:       "rebalance",
	})
	if err != nil {
		t.Fatalf("SubmitMigration: %v", err)
	}
	if mig.State != MigrationStatePending {
		t.Fatalf("expected pending, got %s", mig.State)
	}
	if mig.Reason != "rebalance" || mig.SourceNodeID != "node-a" || mig.TargetNodeID != "node-b" {
		t.Fatalf("migration fields not persisted: %+v", mig)
	}

	// Before completion the owner must stay on the source node.
	view, err := svc.GetShard("shard-1")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if view.OwnerNodeID != "node-a" {
		t.Fatalf("owner changed before completion: %s", view.OwnerNodeID)
	}
	if view.EffectiveStatus != ShardStatusMigrating {
		t.Fatalf("expected migrating status, got %s", view.EffectiveStatus)
	}
	if view.PendingTargetNodeID != "node-b" {
		t.Fatalf("pending target not reported: %q", view.PendingTargetNodeID)
	}

	// Completing before start is illegal.
	if _, err := svc.CompleteMigration(mig.ID); !errors.Is(err, ErrInvalidMigrationState) {
		t.Fatalf("expected ErrInvalidMigrationState, got %v", err)
	}

	if _, err := svc.StartMigration(mig.ID); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}
	view, err = svc.GetShard("shard-1")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if view.OwnerNodeID != "node-a" || view.EffectiveStatus != ShardStatusMigrating {
		t.Fatalf("in-progress migration leaked target as owner: %+v", view)
	}

	done, err := svc.CompleteMigration(mig.ID)
	if err != nil {
		t.Fatalf("CompleteMigration: %v", err)
	}
	if done.State != MigrationStateCompleted {
		t.Fatalf("expected completed, got %s", done.State)
	}
	view, err = svc.GetShard("shard-1")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if view.OwnerNodeID != "node-b" || view.EffectiveStatus != ShardStatusStable {
		t.Fatalf("ownership not switched after completion: %+v", view)
	}

	// Terminal states reject further transitions.
	if _, err := svc.StartMigration(mig.ID); !errors.Is(err, ErrInvalidMigrationState) {
		t.Fatalf("expected ErrInvalidMigrationState, got %v", err)
	}
	if _, err := svc.FailMigration(mig.ID, "too late"); !errors.Is(err, ErrInvalidMigrationState) {
		t.Fatalf("expected ErrInvalidMigrationState, got %v", err)
	}
}

func TestSubmitMigrationValidatesTargetAndSource(t *testing.T) {
	svc := newServiceWithTopology(t)
	if err := svc.UpdateNodeStatus("node-c", NodeStatusDraining); err != nil {
		t.Fatalf("UpdateNodeStatus: %v", err)
	}

	cases := []struct {
		name string
		req  MigrationRequest
		want error
	}{
		{"unknown shard", MigrationRequest{ShardID: "ghost", SourceNodeID: "node-a", TargetNodeID: "node-b"}, ErrShardNotFound},
		{"unknown target", MigrationRequest{ShardID: "shard-1", SourceNodeID: "node-a", TargetNodeID: "ghost"}, ErrNodeNotFound},
		{"same source and target", MigrationRequest{ShardID: "shard-1", SourceNodeID: "node-a", TargetNodeID: "node-a"}, ErrSameSourceTarget},
		{"source not owner", MigrationRequest{ShardID: "shard-1", SourceNodeID: "node-b", TargetNodeID: "node-c"}, ErrSourceNotOwner},
		{"target draining", MigrationRequest{ShardID: "shard-1", SourceNodeID: "node-a", TargetNodeID: "node-c"}, ErrNodeCannotReceive},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := svc.SubmitMigration(tc.req); !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, err)
			}
		})
	}

	if err := svc.UpdateNodeStatus("node-b", NodeStatusOffline); err != nil {
		t.Fatalf("UpdateNodeStatus: %v", err)
	}
	_, err := svc.SubmitMigration(MigrationRequest{ShardID: "shard-1", SourceNodeID: "node-a", TargetNodeID: "node-b"})
	if !errors.Is(err, ErrNodeCannotReceive) {
		t.Fatalf("offline target should be rejected, got %v", err)
	}
}

func TestDuplicateMigrationSubmission(t *testing.T) {
	svc := newServiceWithTopology(t)
	req := MigrationRequest{
		RequestKey:   "req-dup",
		ShardID:      "shard-1",
		SourceNodeID: "node-a",
		TargetNodeID: "node-b",
		Reason:       "rebalance",
	}
	first, err := svc.SubmitMigration(req)
	if err != nil {
		t.Fatalf("SubmitMigration: %v", err)
	}

	// Same request key returns the original task, even after it finishes.
	again, err := svc.SubmitMigration(req)
	if err != nil {
		t.Fatalf("duplicate submit should return original task, got %v", err)
	}
	if again.ID != first.ID {
		t.Fatalf("expected original task %s, got %s", first.ID, again.ID)
	}
	if len(svc.ListMigrations("shard-1")) != 1 {
		t.Fatalf("duplicate submission created extra tasks")
	}

	// A different request for the same shard is rejected while one is active.
	_, err = svc.SubmitMigration(MigrationRequest{
		RequestKey:   "req-other",
		ShardID:      "shard-1",
		SourceNodeID: "node-a",
		TargetNodeID: "node-c",
	})
	if !errors.Is(err, ErrShardMigrationActive) {
		t.Fatalf("expected ErrShardMigrationActive, got %v", err)
	}

	// After the active migration fails, a new migration may be submitted.
	if _, err := svc.FailMigration(first.ID, "target disk full"); err != nil {
		t.Fatalf("FailMigration: %v", err)
	}
	retry, err := svc.SubmitMigration(MigrationRequest{
		RequestKey:   "req-retry",
		ShardID:      "shard-1",
		SourceNodeID: "node-a",
		TargetNodeID: "node-c",
	})
	if err != nil {
		t.Fatalf("submit after failure: %v", err)
	}
	if retry.ID == first.ID {
		t.Fatalf("expected a new task after failure")
	}
}

func TestFailMigrationKeepsOwnershipAndReason(t *testing.T) {
	svc := newServiceWithTopology(t)
	mig, err := svc.SubmitMigration(MigrationRequest{
		ShardID:      "shard-1",
		SourceNodeID: "node-a",
		TargetNodeID: "node-b",
		Reason:       "rebalance",
	})
	if err != nil {
		t.Fatalf("SubmitMigration: %v", err)
	}
	if _, err := svc.StartMigration(mig.ID); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}

	if _, err := svc.FailMigration(mig.ID, ""); !errors.Is(err, ErrEmptyFailureReason) {
		t.Fatalf("expected ErrEmptyFailureReason, got %v", err)
	}

	failed, err := svc.FailMigration(mig.ID, "network partition")
	if err != nil {
		t.Fatalf("FailMigration: %v", err)
	}
	if failed.State != MigrationStateFailed || failed.FailureReason != "network partition" {
		t.Fatalf("failure not recorded: %+v", failed)
	}

	// Failure reason stays visible through queries.
	stored, err := svc.GetMigration(mig.ID)
	if err != nil {
		t.Fatalf("GetMigration: %v", err)
	}
	if stored.FailureReason != "network partition" {
		t.Fatalf("failure reason lost: %+v", stored)
	}

	// Ownership never left the source node and the shard is stable again.
	view, err := svc.GetShard("shard-1")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if view.OwnerNodeID != "node-a" || view.EffectiveStatus != ShardStatusStable {
		t.Fatalf("failed migration changed ownership: %+v", view)
	}

	// Recovery: a fresh migration to the same target succeeds.
	recovery, err := svc.SubmitMigration(MigrationRequest{
		ShardID:      "shard-1",
		SourceNodeID: "node-a",
		TargetNodeID: "node-b",
		Reason:       "retry after failure",
	})
	if err != nil {
		t.Fatalf("recovery submit: %v", err)
	}
	if _, err := svc.StartMigration(recovery.ID); err != nil {
		t.Fatalf("recovery start: %v", err)
	}
	if _, err := svc.CompleteMigration(recovery.ID); err != nil {
		t.Fatalf("recovery complete: %v", err)
	}
	view, err = svc.GetShard("shard-1")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if view.OwnerNodeID != "node-b" {
		t.Fatalf("recovery did not move ownership: %+v", view)
	}
}

func TestOfflineNodeReportedExplicitly(t *testing.T) {
	svc := newServiceWithTopology(t)
	if err := svc.UpdateNodeStatus("node-a", NodeStatusOffline); err != nil {
		t.Fatalf("UpdateNodeStatus: %v", err)
	}

	node, err := svc.GetNode("node-a")
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if node.Status != NodeStatusOffline {
		t.Fatalf("expected offline node, got %s", node.Status)
	}

	view, err := svc.GetShard("shard-1")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if view.EffectiveStatus != ShardStatusOwnerOffline {
		t.Fatalf("expected owner_offline, got %s", view.EffectiveStatus)
	}
	if view.OwnerNodeID != "node-a" {
		t.Fatalf("offline node must remain the owner, got %s", view.OwnerNodeID)
	}

	if err := svc.UpdateNodeStatus("node-a", "bogus"); !errors.Is(err, ErrInvalidNodeStatus) {
		t.Fatalf("expected ErrInvalidNodeStatus, got %v", err)
	}
}

func TestShardHistoryTracesOwnershipMoves(t *testing.T) {
	svc := newServiceWithTopology(t)

	mig, err := svc.SubmitMigration(MigrationRequest{
		ShardID:      "shard-1",
		SourceNodeID: "node-a",
		TargetNodeID: "node-b",
		Reason:       "rebalance",
	})
	if err != nil {
		t.Fatalf("SubmitMigration: %v", err)
	}
	if _, err := svc.StartMigration(mig.ID); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}
	if _, err := svc.CompleteMigration(mig.ID); err != nil {
		t.Fatalf("CompleteMigration: %v", err)
	}

	// Move the shard back to node-a after the first move completed.
	second, err := svc.SubmitMigration(MigrationRequest{
		ShardID:      "shard-1",
		SourceNodeID: "node-b",
		TargetNodeID: "node-a",
		Reason:       "move back",
	})
	if err != nil {
		t.Fatalf("second SubmitMigration: %v", err)
	}
	if _, err := svc.StartMigration(second.ID); err != nil {
		t.Fatalf("second StartMigration: %v", err)
	}
	if _, err := svc.FailMigration(second.ID, "aborted"); err != nil {
		t.Fatalf("second FailMigration: %v", err)
	}

	history, err := svc.ShardHistory("shard-1")
	if err != nil {
		t.Fatalf("ShardHistory: %v", err)
	}
	wantEvents := []HistoryEventType{
		HistoryEventShardCreated,
		HistoryEventMigrationStarted,
		HistoryEventMigrationComplete,
		HistoryEventMigrationStarted,
		HistoryEventMigrationFailed,
	}
	if len(history) != len(wantEvents) {
		t.Fatalf("expected %d history entries, got %d: %+v", len(wantEvents), len(history), history)
	}
	for i, want := range wantEvents {
		if history[i].Event != want {
			t.Fatalf("entry %d: expected %s, got %s", i, want, history[i].Event)
		}
	}
	if history[2].FromNodeID != "node-a" || history[2].ToNodeID != "node-b" {
		t.Fatalf("completed move not traced: %+v", history[2])
	}
	if history[4].Detail != "aborted" {
		t.Fatalf("failure detail not traced: %+v", history[4])
	}

	// Ownership ended on node-b because the second migration failed.
	view, err := svc.GetShard("shard-1")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if view.OwnerNodeID != "node-b" {
		t.Fatalf("expected owner node-b, got %s", view.OwnerNodeID)
	}

	if _, err := svc.ShardHistory("ghost"); !errors.Is(err, ErrShardNotFound) {
		t.Fatalf("expected ErrShardNotFound, got %v", err)
	}
}
