package goshardreassignment

import (
	"errors"
	"testing"
)

func newServiceWithNodes(t *testing.T, ids ...string) *Service {
	t.Helper()
	svc := NewService()
	for _, id := range ids {
		if _, err := svc.CreateNode(id, NodeStatusActive); err != nil {
			t.Fatalf("CreateNode(%s): %v", id, err)
		}
	}
	return svc
}

func TestCreateNodeAndShardValidation(t *testing.T) {
	svc := newServiceWithNodes(t, "node-a")

	if _, err := svc.CreateNode("node-a", NodeStatusActive); !errors.Is(err, ErrNodeExists) {
		t.Fatalf("duplicate node: got %v, want ErrNodeExists", err)
	}
	if _, err := svc.CreateNode("bad", "weird"); err == nil {
		t.Fatal("invalid node status accepted")
	}
	if _, err := svc.CreateShard("shard-1", 100, "node-a"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}
	if _, err := svc.CreateShard("shard-1", 100, "node-a"); !errors.Is(err, ErrShardExists) {
		t.Fatalf("duplicate shard: got %v, want ErrShardExists", err)
	}
	if _, err := svc.CreateShard("shard-2", 100, "ghost"); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("unknown owner: got %v, want ErrNodeNotFound", err)
	}
	if _, err := svc.CreateShard("shard-3", 0, "node-a"); !errors.Is(err, ErrInvalidCapacity) {
		t.Fatalf("zero capacity: got %v, want ErrInvalidCapacity", err)
	}
}

func TestMigrationLifecycleMovesOwnership(t *testing.T) {
	svc := newServiceWithNodes(t, "node-a", "node-b")
	if _, err := svc.CreateShard("shard-1", 100, "node-a"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	migration, err := svc.SubmitMigration("shard-1", "node-b", "rebalance")
	if err != nil {
		t.Fatalf("SubmitMigration: %v", err)
	}
	if migration.Status != MigrationStatusPending {
		t.Fatalf("status = %s, want pending", migration.Status)
	}
	if migration.SourceNodeID != "node-a" || migration.TargetNodeID != "node-b" {
		t.Fatalf("unexpected endpoints: %+v", migration)
	}

	// Completing before start is rejected.
	if _, err := svc.CompleteMigration(migration.ID); err == nil {
		t.Fatal("complete before start accepted")
	}

	// Ownership stays with the source until completion.
	view, err := svc.GetShard("shard-1")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if view.OwnerID != "node-a" {
		t.Fatalf("owner changed early to %s", view.OwnerID)
	}
	if view.Status != ShardStatusMigrating {
		t.Fatalf("status = %s, want migrating", view.Status)
	}

	if _, err := svc.StartMigration(migration.ID); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}
	done, err := svc.CompleteMigration(migration.ID)
	if err != nil {
		t.Fatalf("CompleteMigration: %v", err)
	}
	if done.Status != MigrationStatusCompleted {
		t.Fatalf("status = %s, want completed", done.Status)
	}

	view, err = svc.GetShard("shard-1")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if view.OwnerID != "node-b" || view.Status != ShardStatusActive {
		t.Fatalf("after completion: owner=%s status=%s", view.OwnerID, view.Status)
	}

	// History traces the move from node-a to node-b.
	history := svc.ShardHistory("shard-1")
	var kinds []HistoryEventKind
	for _, entry := range history {
		kinds = append(kinds, entry.Kind)
	}
	want := []HistoryEventKind{
		HistoryShardCreated,
		HistoryMigrationSubmitted,
		HistoryMigrationStarted,
		HistoryMigrationCompleted,
	}
	if len(kinds) != len(want) {
		t.Fatalf("history kinds = %v, want %v", kinds, want)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("history kinds = %v, want %v", kinds, want)
		}
	}
	last := history[len(history)-1]
	if last.FromNodeID != "node-a" || last.ToNodeID != "node-b" {
		t.Fatalf("last history entry = %+v", last)
	}
}

func TestDuplicateAndConflictingMigrations(t *testing.T) {
	svc := newServiceWithNodes(t, "node-a", "node-b", "node-c")
	if _, err := svc.CreateShard("shard-1", 100, "node-a"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	first, err := svc.SubmitMigration("shard-1", "node-b", "rebalance")
	if err != nil {
		t.Fatalf("SubmitMigration: %v", err)
	}
	// Same request returns the original task.
	again, err := svc.SubmitMigration("shard-1", "node-b", "rebalance")
	if err != nil {
		t.Fatalf("resubmit: %v", err)
	}
	if again.ID != first.ID {
		t.Fatalf("resubmit created %s, want original %s", again.ID, first.ID)
	}
	// A different target on the same shard is rejected while one is active.
	if _, err := svc.SubmitMigration("shard-1", "node-c", "rebalance"); err == nil {
		t.Fatal("conflicting migration accepted")
	}
	// Same target but different reason is also a different request.
	if _, err := svc.SubmitMigration("shard-1", "node-b", "other-reason"); err == nil {
		t.Fatal("migration with different reason accepted while active")
	}

	if _, err := svc.FailMigration(first.ID, "target disk full"); err != nil {
		t.Fatalf("FailMigration: %v", err)
	}
	// After failure the slot is free again.
	second, err := svc.SubmitMigration("shard-1", "node-c", "rebalance")
	if err != nil {
		t.Fatalf("SubmitMigration after failure: %v", err)
	}
	if second.ID == first.ID {
		t.Fatal("expected a new migration task after failure")
	}
}

func TestFailedMigrationKeepsOwnershipAndRecordsReason(t *testing.T) {
	svc := newServiceWithNodes(t, "node-a", "node-b")
	if _, err := svc.CreateShard("shard-1", 100, "node-a"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}
	migration, err := svc.SubmitMigration("shard-1", "node-b", "rebalance")
	if err != nil {
		t.Fatalf("SubmitMigration: %v", err)
	}
	if _, err := svc.StartMigration(migration.ID); err != nil {
		t.Fatalf("StartMigration: %v", err)
	}
	failed, err := svc.FailMigration(migration.ID, "network partition")
	if err != nil {
		t.Fatalf("FailMigration: %v", err)
	}
	if failed.Status != MigrationStatusFailed || failed.FailReason != "network partition" {
		t.Fatalf("failed migration = %+v", failed)
	}

	// Failure reason is visible via query.
	stored, err := svc.GetMigration(migration.ID)
	if err != nil {
		t.Fatalf("GetMigration: %v", err)
	}
	if stored.FailReason != "network partition" {
		t.Fatalf("stored fail reason = %q", stored.FailReason)
	}

	// Ownership never left the source node.
	view, err := svc.GetShard("shard-1")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if view.OwnerID != "node-a" || view.Status != ShardStatusActive {
		t.Fatalf("after failure: owner=%s status=%s", view.OwnerID, view.Status)
	}

	// Terminal migrations cannot transition further.
	if _, err := svc.StartMigration(migration.ID); err == nil {
		t.Fatal("start after failure accepted")
	}
	if _, err := svc.CompleteMigration(migration.ID); err == nil {
		t.Fatal("complete after failure accepted")
	}

	// Recovery: a fresh migration can complete successfully.
	retry, err := svc.SubmitMigration("shard-1", "node-b", "retry after failure")
	if err != nil {
		t.Fatalf("retry SubmitMigration: %v", err)
	}
	if _, err := svc.StartMigration(retry.ID); err != nil {
		t.Fatalf("retry StartMigration: %v", err)
	}
	if _, err := svc.CompleteMigration(retry.ID); err != nil {
		t.Fatalf("retry CompleteMigration: %v", err)
	}
	view, err = svc.GetShard("shard-1")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if view.OwnerID != "node-b" {
		t.Fatalf("owner after recovery = %s", view.OwnerID)
	}
}

func TestTargetNodeMustBeReceivable(t *testing.T) {
	svc := newServiceWithNodes(t, "node-a", "node-b")
	if err := svc.SetNodeStatus("node-b", NodeStatusDraining); err != nil {
		t.Fatalf("SetNodeStatus: %v", err)
	}
	if _, err := svc.CreateShard("shard-1", 100, "node-a"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}
	if _, err := svc.SubmitMigration("shard-1", "node-b", "rebalance"); err == nil {
		t.Fatal("migration to draining node accepted")
	}
	if err := svc.SetNodeStatus("node-b", NodeStatusOffline); err != nil {
		t.Fatalf("SetNodeStatus: %v", err)
	}
	if _, err := svc.SubmitMigration("shard-1", "node-b", "rebalance"); err == nil {
		t.Fatal("migration to offline node accepted")
	}
	if _, err := svc.SubmitMigration("shard-1", "ghost", "rebalance"); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("unknown target: got %v, want ErrNodeNotFound", err)
	}
	if _, err := svc.SubmitMigration("shard-1", "node-a", "rebalance"); err == nil {
		t.Fatal("migration to current owner accepted")
	}
}

func TestOfflineNodeAndMigratingShardQueries(t *testing.T) {
	svc := newServiceWithNodes(t, "node-a", "node-b")
	if _, err := svc.CreateShard("shard-1", 100, "node-a"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}

	migration, err := svc.SubmitMigration("shard-1", "node-b", "rebalance")
	if err != nil {
		t.Fatalf("SubmitMigration: %v", err)
	}
	view, err := svc.GetShard("shard-1")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if view.Status != ShardStatusMigrating {
		t.Fatalf("status = %s, want migrating", view.Status)
	}
	if view.Migration == nil || view.Migration.ID != migration.ID {
		t.Fatalf("view migration = %+v", view.Migration)
	}
	// The temporary target must not show up as owner.
	if view.OwnerID != "node-a" {
		t.Fatalf("owner = %s during migration", view.OwnerID)
	}

	if err := svc.SetNodeStatus("node-a", NodeStatusOffline); err != nil {
		t.Fatalf("SetNodeStatus: %v", err)
	}
	node, err := svc.GetNode("node-a")
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if node.Status != NodeStatusOffline {
		t.Fatalf("node status = %s, want offline", node.Status)
	}
	view, err = svc.GetShard("shard-1")
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if view.Status != ShardStatusOffline {
		t.Fatalf("status = %s, want offline", view.Status)
	}
}

func TestListQueries(t *testing.T) {
	svc := newServiceWithNodes(t, "node-a", "node-b")
	if _, err := svc.CreateShard("shard-1", 100, "node-a"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}
	if _, err := svc.CreateShard("shard-2", 200, "node-b"); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}
	if _, err := svc.SubmitMigration("shard-1", "node-b", "rebalance"); err != nil {
		t.Fatalf("SubmitMigration: %v", err)
	}

	shards, err := svc.ListShards()
	if err != nil {
		t.Fatalf("ListShards: %v", err)
	}
	if len(shards) != 2 || shards[0].ShardID != "shard-1" || shards[1].ShardID != "shard-2" {
		t.Fatalf("ListShards = %+v", shards)
	}
	if nodes := svc.ListNodes(); len(nodes) != 2 {
		t.Fatalf("ListNodes = %+v", nodes)
	}
	if migrations := svc.ListMigrations(); len(migrations) != 1 {
		t.Fatalf("ListMigrations = %+v", migrations)
	}
	if history := svc.History(); len(history) == 0 {
		t.Fatal("History is empty")
	}
}
