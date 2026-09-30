package goshardreassignment

import (
	"fmt"
	"sort"
)

// buildPlanSteps derives the mandatory ordered steps for a migration.
//
// Ordering guarantees:
//  1. every target replica joins (add) before it must catch up;
//  2. every newly added target replica finishes syncing before any role
//     switch or old-replica removal, so the healthy safety line is never
//     reduced;
//  3. the primary role is switched at most once;
//  4. old replicas are removed only after the switch point, so a primary is
//     never removed while still serving writes.
//
// BaseVersion is stamped per step: all add/sync steps run against the frozen
// start version; each mutating step afterwards addresses the version produced
// by the previous one. Receipts must echo it exactly.
func buildPlanSteps(start ShardConfig, target []Replica, targetPrimary string) []Step {
	var adds, syncs []string
	var removes []string

	targetSet := map[string]bool{}
	for _, r := range target {
		targetSet[r.NodeID] = true
		if !start.HasReplica(r.NodeID) {
			adds = append(adds, r.NodeID)
			syncs = append(syncs, r.NodeID)
		}
	}
	// A pre-existing target replica that will become primary must also prove
	// it is caught up before promotion.
	if targetPrimary != start.PrimaryID && start.HasReplica(targetPrimary) {
		syncs = append(syncs, targetPrimary)
	}
	for _, r := range start.Replicas {
		if !targetSet[r.NodeID] {
			removes = append(removes, r.NodeID)
		}
	}
	sort.Strings(adds)
	sort.Strings(syncs)
	sort.Strings(removes)

	var steps []Step
	version := start.Version
	addStep := func(op Op, nodeID string, mutates bool) {
		steps = append(steps, Step{
			Index:       len(steps),
			Op:          op,
			NodeID:      nodeID,
			BaseVersion: version,
		})
		if mutates {
			version++
		}
	}

	for _, n := range adds {
		addStep(OpAdd, n, true)
	}
	for _, n := range syncs {
		addStep(OpSync, n, false)
	}
	if targetPrimary != start.PrimaryID {
		addStep(OpPromote, targetPrimary, true)
	}
	for _, n := range removes {
		addStep(OpRemove, n, true)
	}
	return steps
}

// validateTarget checks the frozen plan against safety invariants:
// primary must exist, replica IDs must be unique and healthy assumptions
// must keep at least MinHealthy healthy replicas at every intermediate
// configuration.
func validateTarget(start ShardConfig, target []Replica, targetPrimary string, minHealthy int) error {
	if len(target) == 0 {
		return fmt.Errorf("%w: empty target replica set", ErrInvalidArgument)
	}
	seen := map[string]bool{}
	primaries := 0
	for _, r := range target {
		if r.NodeID == "" {
			return fmt.Errorf("%w: empty replica node id", ErrInvalidArgument)
		}
		if seen[r.NodeID] {
			return fmt.Errorf("%w: duplicate target replica %q", ErrInvalidArgument, r.NodeID)
		}
		seen[r.NodeID] = true
		if r.Primary {
			primaries++
		}
	}
	if !seen[targetPrimary] {
		return fmt.Errorf("%w: target primary %q is not in target replicas", ErrInvalidArgument, targetPrimary)
	}
	if primaries > 1 {
		return fmt.Errorf("%w: target declares multiple primaries", ErrInvalidArgument)
	}

	// Simulate the whole sequence configuration by configuration.
	cfg := cloneConfig(start)
	cfg.MinHealthy = minHealthy
	healthyWith := func(cfg ShardConfig) bool {
		// Joining replicas are unhealthy until synced; the simulation below
		// marks them healthy only after their sync step.
		return cfg.HealthyCount() >= cfg.MinHealthy
	}
	if !healthyWith(cfg) {
		return fmt.Errorf("%w: starting configuration has %d healthy but min is %d",
			ErrSafetyViolation, cfg.HealthyCount(), minHealthy)
	}

	synced := map[string]bool{}
	for _, r := range start.Replicas {
		if r.State == ReplicaHealthy {
			synced[r.NodeID] = true
		}
	}
	for _, step := range buildPlanSteps(start, target, targetPrimary) {
		switch step.Op {
		case OpAdd:
			cfg.Replicas = append(cfg.Replicas, Replica{NodeID: step.NodeID, State: ReplicaUnhealthy})
		case OpSync:
			synced[step.NodeID] = true
			for i := range cfg.Replicas {
				if cfg.Replicas[i].NodeID == step.NodeID {
					cfg.Replicas[i].State = ReplicaHealthy
				}
			}
		case OpPromote:
			for i := range cfg.Replicas {
				cfg.Replicas[i].Primary = cfg.Replicas[i].NodeID == step.NodeID
			}
			cfg.PrimaryID = step.NodeID
		case OpRemove:
			next := cfg.Replicas[:0]
			for _, r := range cfg.Replicas {
				if r.NodeID != step.NodeID {
					next = append(next, r)
				}
			}
			cfg.Replicas = next
		}
		if !healthyWith(cfg) {
			return fmt.Errorf("%w: after %s of %q only %d of %d healthy replicas remain",
				ErrSafetyViolation, step.Op, step.NodeID, cfg.HealthyCount(), minHealthy)
		}
	}
	return nil
}
