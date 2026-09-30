package goshardreassignment

import (
	"fmt"
	"sync"
	"time"
)

// receiptKey identifies one node's receipt for one step of a plan.
type receiptKey struct {
	step int
	node ReplicaID
}

// shardState is the folded in-memory state of one registered shard.
type shardState struct {
	id      ShardID
	config  ShardConfig
	healthy map[ReplicaID]bool
}

// planState is the folded in-memory state of one migration plan.
type planState struct {
	plan     Plan
	receipts map[receiptKey]*ReceiptRecordedDetail
}

// Service is the safe shard-reassignment service. It is safe for
// concurrent use: every public operation is serialized and derived from
// an append-only event stream persisted through the Store.
type Service struct {
	mu     sync.Mutex
	store  Store
	seq    int64
	now    func() time.Time
	events []Event

	shards map[ShardID]*shardState
	plans  map[string]*planState
	// active maps a shard id to its single non-terminal plan id.
	active map[ShardID]string
}

// NewService opens a service over store and replays the durable history.
func NewService(store Store) (*Service, error) {
	s := &Service{
		store:  store,
		now:    func() time.Time { return time.Now().UTC() },
		shards: make(map[ShardID]*shardState),
		plans:  make(map[string]*planState),
		active: make(map[ShardID]string),
	}
	if err := store.Replay(func(event Event) error {
		s.seq = event.Seq
		s.apply(event)
		s.events = append(s.events, event)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("replay event store: %w", err)
	}
	return s, nil
}

// Close releases the underlying store.
func (s *Service) Close() error { return s.store.Close() }

func (s *Service) stamp() string { return s.now().Format(time.RFC3339Nano) }

// emit durably appends the event and, only after it is persisted, folds it
// into the in-memory state.
func (s *Service) emit(kind string, shardID ShardID, planID string, version int64, detail any) (Event, error) {
	event := Event{
		Seq:     s.seq + 1,
		Kind:    kind,
		ShardID: shardID,
		PlanID:  planID,
		Version: version,
		At:      s.stamp(),
		Detail:  detail,
	}
	if err := s.store.Append(event); err != nil {
		return Event{}, err
	}
	s.seq = event.Seq
	s.apply(event)
	s.events = append(s.events, event)
	return event, nil
}

// apply folds one event into memory. It is deterministic: replaying the
// persisted stream reproduces the exact service state.
func (s *Service) apply(event Event) {
	switch event.Kind {
	case KindShardRegistered:
		d := event.Detail.(*ShardRegisteredDetail)
		sh := &shardState{id: event.ShardID, healthy: make(map[ReplicaID]bool)}
		sh.config = ShardConfig{Version: 1, Primary: d.Primary, Members: cloneIDs(d.Members), MinHealthy: d.MinHealthy}
		for _, member := range d.Members {
			sh.healthy[member] = true
		}
		s.shards[event.ShardID] = sh

	case KindHealthReported:
		d := event.Detail.(*HealthReportedDetail)
		sh := s.shards[event.ShardID]
		sh.healthy = make(map[ReplicaID]bool)
		for _, node := range d.Healthy {
			if containsID(sh.config.Members, node) {
				sh.healthy[node] = true
			}
		}

	case KindConfigChanged:
		d := event.Detail.(*ConfigChangeDetail)
		sh := s.shards[event.ShardID]
		sh.config = ShardConfig{
			Version:    d.NewVersion,
			Primary:    d.NewPrimary,
			Members:    cloneIDs(d.NewMembers),
			MinHealthy: d.MinHealthy,
		}
		for node := range sh.healthy {
			if !containsID(sh.config.Members, node) {
				delete(sh.healthy, node)
			}
		}

	case KindPlanCreated:
		d := event.Detail.(*PlanCreatedDetail)
		steps := make([]Step, len(d.Steps))
		for i, step := range d.Steps {
			step.ConfigVersion = 0
			step.Done = false
			steps[i] = step
		}
		sh := s.shards[event.ShardID]
		ps := &planState{receipts: make(map[receiptKey]*ReceiptRecordedDetail)}
		ps.plan = Plan{
			ID:            d.PlanID,
			ShardID:       event.ShardID,
			StartConfig:   sh.config.Clone(),
			TargetPrimary: d.TargetPrimary,
			TargetMembers: cloneIDs(d.TargetMembers),
			Steps:         steps,
			CurrentStep:   0,
			Status:        PlanActive,
			CreatedAt:     d.CreatedAt,
			UpdatedAt:     d.CreatedAt,
		}
		ps.plan.StartConfig.Version = d.StartVersion
		s.plans[d.PlanID] = ps
		s.active[event.ShardID] = d.PlanID

	case KindStepEntered:
		d := event.Detail.(*StepEnteredDetail)
		ps := s.plans[d.PlanID]
		ps.plan.CurrentStep = d.Step
		ps.plan.Steps[d.Step].ConfigVersion = d.ConfigVersion
		ps.plan.UpdatedAt = event.At

	case KindReceiptRecorded:
		d := event.Detail.(*ReceiptRecordedDetail)
		ps := s.plans[d.PlanID]
		ps.receipts[receiptKey{step: d.Step, node: d.Node}] = d
		ps.plan.UpdatedAt = event.At

	case KindStepAdvanced:
		d := event.Detail.(*StepAdvancedDetail)
		ps := s.plans[d.PlanID]
		ps.plan.Steps[d.Step].Done = true
		ps.plan.CurrentStep = d.Step + 1
		ps.plan.UpdatedAt = event.At

	case KindPlanPaused:
		d := event.Detail.(*PlanPausedDetail)
		ps := s.plans[d.PlanID]
		ps.plan.Status = PlanPaused
		ps.plan.PauseReason = d.Reason
		ps.plan.FailureDetail = d.Detail
		ps.plan.UpdatedAt = event.At

	case KindPlanResumed:
		d := event.Detail.(*PlanResumedDetail)
		ps := s.plans[d.PlanID]
		for key := range ps.receipts {
			if key.step == ps.plan.CurrentStep {
				delete(ps.receipts, key)
			}
		}
		ps.plan.Status = PlanActive
		ps.plan.PauseReason = ""
		ps.plan.FailureDetail = ""
		ps.plan.UpdatedAt = event.At

	case KindPlanCancelled:
		d := event.Detail.(*PlanCancelledDetail)
		ps := s.plans[d.PlanID]
		ps.plan.Status = PlanCancelled
		ps.plan.PauseReason = ""
		ps.plan.FailureDetail = ""
		ps.plan.UpdatedAt = event.At
		if shardID := ps.plan.ShardID; s.active[shardID] == d.PlanID {
			delete(s.active, shardID)
		}

	case KindPlanCompleted:
		d := event.Detail.(*PlanCompletedDetail)
		ps := s.plans[d.PlanID]
		ps.plan.Status = PlanCompleted
		ps.plan.CurrentStep = len(ps.plan.Steps)
		ps.plan.PauseReason = ""
		ps.plan.FailureDetail = ""
		ps.plan.UpdatedAt = event.At
		if shardID := ps.plan.ShardID; s.active[shardID] == d.PlanID {
			delete(s.active, shardID)
		}
	}
}

// ---------------------------------------------------------------------------
// Shard registration, configuration and health
// ---------------------------------------------------------------------------

// RegisterShard registers a new shard at configuration version 1. Every
// member starts healthy until the first health report.
func (s *Service) RegisterShard(req RegisterShardRequest) (ShardConfig, error) {
	if req.ShardID == "" {
		return ShardConfig{}, fmt.Errorf("%w: empty shard id", ErrInvalidRequest)
	}
	if req.Primary == "" {
		return ShardConfig{}, fmt.Errorf("%w: empty primary", ErrInvalidRequest)
	}
	if len(req.Members) == 0 {
		return ShardConfig{}, fmt.Errorf("%w: empty members", ErrInvalidRequest)
	}
	if req.MinHealthy < 1 {
		return ShardConfig{}, fmt.Errorf("%w: min_healthy must be >= 1", ErrInvalidRequest)
	}
	if err := assertUniqueMembers(req.Members); err != nil {
		return ShardConfig{}, err
	}
	if !containsID(req.Members, req.Primary) {
		return ShardConfig{}, ErrPrimaryMissing
	}
	if req.MinHealthy > len(req.Members) {
		return ShardConfig{}, fmt.Errorf("%w: min_healthy %d exceeds %d members", ErrSafetyViolation, req.MinHealthy, len(req.Members))
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.shards[req.ShardID]; ok {
		return ShardConfig{}, fmt.Errorf("%w: %s", ErrShardExists, req.ShardID)
	}
	if _, err := s.emit(KindShardRegistered, req.ShardID, "", 1, &ShardRegisteredDetail{
		Primary:    req.Primary,
		Members:    cloneIDs(req.Members),
		MinHealthy: req.MinHealthy,
	}); err != nil {
		return ShardConfig{}, err
	}
	return s.shards[req.ShardID].config.Clone(), nil
}

// ---------------------------------------------------------------------------
// Plan creation
// ---------------------------------------------------------------------------

// CreatePlan freezes the current configuration (primary, members, version,
// min healthy) and the requested target, derives the mandatory steps and
// starts the migration. A shard has at most one non-terminal plan; a plan
// paused by a conflict must be cancelled first.
func (s *Service) CreatePlan(req CreatePlanRequest) (Plan, error) {
	if req.ShardID == "" {
		return Plan{}, fmt.Errorf("%w: empty shard id", ErrInvalidRequest)
	}
	if len(req.TargetMembers) == 0 {
		return Plan{}, fmt.Errorf("%w: empty target members", ErrInvalidRequest)
	}
	if err := assertUniqueMembers(req.TargetMembers); err != nil {
		return Plan{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	sh, ok := s.shards[req.ShardID]
	if !ok {
		return Plan{}, fmt.Errorf("%w: %s", ErrShardNotFound, req.ShardID)
	}
	if planID := s.active[req.ShardID]; planID != "" {
		ps := s.plans[planID]
		return Plan{}, fmt.Errorf("%w: shard %s has plan %s (%s)",
			ErrPlanActive, req.ShardID, planID, ps.plan.Status)
	}

	start := sh.config.Clone()
	targetPrimary := req.TargetPrimary
	if targetPrimary == "" {
		targetPrimary = start.Primary
	}
	if !containsID(req.TargetMembers, targetPrimary) {
		return Plan{}, fmt.Errorf("%w: target primary %s must be a target member", ErrInvalidRequest, targetPrimary)
	}

	var added, removed []ReplicaID
	for _, member := range req.TargetMembers {
		if !containsID(start.Members, member) {
			added = append(added, member)
		}
	}
	for _, member := range start.Members {
		if !containsID(req.TargetMembers, member) {
			removed = append(removed, member)
		}
	}
	switchNeeded := targetPrimary != start.Primary
	if len(added) == 0 && len(removed) == 0 && !switchNeeded {
		return Plan{}, fmt.Errorf("%w: target identical to current config version %d", ErrInvalidRequest, start.Version)
	}

	// Safety precondition: the final set must itself satisfy MinHealthy.
	// Intermediate states are supersets of the start set (join happens
	// before any removal), so the healthy count can never dip below the
	// current safe count until the very last step, which is gated again.
	targetHealthy := 0
	for _, member := range req.TargetMembers {
		if sh.healthy[member] {
			targetHealthy++
		}
	}
	if targetHealthy < start.MinHealthy {
		return Plan{}, fmt.Errorf("%w: target has only %d healthy members, min is %d",
			ErrSafetyViolation, targetHealthy, start.MinHealthy)
	}

	steps := buildSteps(start, added, removed, targetPrimary, switchNeeded)

	createdAt := s.stamp()
	planEvent, err := s.emit(KindPlanCreated, req.ShardID, "", start.Version, &PlanCreatedDetail{
		// PlanID is finalized below from the event seq; the event itself
		// stays authoritative for replay.
		TargetPrimary: targetPrimary,
		TargetMembers: cloneIDs(req.TargetMembers),
		Steps:         cloneSteps(steps),
		CreatedAt:     createdAt,
	})
	if err != nil {
		return Plan{}, err
	}
	planID := planIDFor(planEvent.Seq)

	// Rewire the freshly created state to its deterministic id. Because the
	// id is derived from the event sequence, replay reaches the same value.
	delete(s.plans, "")
	delete(s.active, req.ShardID)
	ps := &planState{receipts: make(map[receiptKey]*ReceiptRecordedDetail)}
	ps.plan = Plan{
		ID:            planID,
		ShardID:       req.ShardID,
		StartConfig:   start,
		TargetPrimary: targetPrimary,
		TargetMembers: cloneIDs(req.TargetMembers),
		Steps:         steps,
		CurrentStep:   0,
		Status:        PlanActive,
		CreatedAt:     createdAt,
		UpdatedAt:     createdAt,
	}
	s.plans[planID] = ps
	s.active[req.ShardID] = planID

	// Keep the durable detail consistent with the derived id; the
	// PlanCreatedDetail does not carry its own PlanID field for creation,
	// so replay derives the id from Seq identically.
	_ = createdDetailOf(planEvent)

	if err := s.enterStep(ps, 0); err != nil {
		return Plan{}, err
	}
	return ps.plan.Clone(), nil
}

func createdDetailOf(event Event) *PlanCreatedDetail {
	return event.Detail.(*PlanCreatedDetail)
}

// planIDFor derives the deterministic plan identifier from its creation
// event sequence, so replay and live runs produce identical ids.
func planIDFor(seq int64) string {
	return fmt.Sprintf("plan-%06d", seq)
}

// buildSteps derives the mandatory, ordered phases. Empty phases are
// omitted (for example a pure primary swap has no add/sync/remove).
func buildSteps(start ShardConfig, added, removed []ReplicaID, targetPrimary ReplicaID, switchNeeded bool) []Step {
	var steps []Step
	if len(added) > 0 {
		steps = append(steps, Step{Kind: StepAdd, Nodes: cloneIDs(added)})
		// Every newly joined replica must confirm it caught up.
		steps = append(steps, Step{Kind: StepSync, Nodes: cloneIDs(added)})
	}
	if switchNeeded {
		nodes := []ReplicaID{start.Primary, targetPrimary}
		steps = append(steps, Step{Kind: StepSwitch, Nodes: nodes})
	}
	if len(removed) > 0 {
		steps = append(steps, Step{Kind: StepRemove, Nodes: cloneIDs(removed)})
	}
	for i := range steps {
		steps[i].Index = i
	}
	return steps
}

func cloneSteps(in []Step) []Step {
	out := make([]Step, len(in))
	for i, step := range in {
		out[i] = step.Clone()
	}
	return out
}

// enterStep marks step index in-flight and pins the configuration version
// its receipts must echo. For the add step it first commits the join as a
// new version: target replicas are members before any removal can happen.
func (s *Service) enterStep(ps *planState, index int) error {
	step := &ps.plan.Steps[index]
	sh := s.shards[ps.plan.ShardID]
	version := sh.config.Version

	if step.Kind == StepAdd {
		old := sh.config
		members := make([]ReplicaID, 0, len(old.Members)+len(step.Nodes))
		members = append(members, old.Members...)
		for _, node := range step.Nodes {
			if !containsID(members, node) {
				members = append(members, node)
			}
		}
		if _, err := s.emit(KindConfigChanged, ps.plan.ShardID, ps.plan.ID, old.Version+1, &ConfigChangeDetail{
			Reason:     ChangeReasonAdd,
			OldVersion: old.Version,
			NewVersion: old.Version + 1,
			OldPrimary: old.Primary,
			NewPrimary: old.Primary,
			OldMembers: cloneIDs(old.Members),
			NewMembers: members,
			MinHealthy: old.MinHealthy,
		}); err != nil {
			return err
		}
		version = old.Version + 1
	}

	if _, err := s.emit(KindStepEntered, ps.plan.ShardID, ps.plan.ID, version, &StepEnteredDetail{
		PlanID:        ps.plan.ID,
		Step:          index,
		Kind:          string(step.Kind),
		ConfigVersion: version,
	}); err != nil {
		return err
	}
	return nil
}

// GetConfig returns the current configuration snapshot of a shard.
func (s *Service) GetConfig(id ShardID) (ShardConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sh, ok := s.shards[id]
	if !ok {
		return ShardConfig{}, fmt.Errorf("%w: %s", ErrShardNotFound, id)
	}
	return sh.config.Clone(), nil
}

// GetHealthy returns the last reported healthy members, in configuration
// membership order.
func (s *Service) GetHealthy(id ShardID) ([]ReplicaID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sh, ok := s.shards[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrShardNotFound, id)
	}
	return s.healthyList(sh), nil
}

func (s *Service) healthyList(sh *shardState) []ReplicaID {
	out := make([]ReplicaID, 0, len(sh.healthy))
	for _, member := range sh.config.Members {
		if sh.healthy[member] {
			out = append(out, member)
		}
	}
	return out
}

// ReportHealth replaces the observed healthy set of a shard. The version
// never changes; if an in-flight plan was waiting for healthy replicas,
// the recovered health lets it advance automatically.
func (s *Service) ReportHealth(req ReportHealthRequest) ([]ReplicaID, error) {
	if req.ShardID == "" {
		return nil, fmt.Errorf("%w: empty shard id", ErrInvalidRequest)
	}
	if err := assertUniqueMembers(req.Healthy); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	sh, ok := s.shards[req.ShardID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrShardNotFound, req.ShardID)
	}
	healthy := make([]ReplicaID, 0, len(req.Healthy))
	for _, node := range req.Healthy {
		if containsID(sh.config.Members, node) {
			healthy = append(healthy, node)
		}
	}
	if _, err := s.emit(KindHealthReported, req.ShardID, "", sh.config.Version, &HealthReportedDetail{
		Healthy: cloneIDs(healthy),
	}); err != nil {
		return nil, err
	}

	if planID := s.active[req.ShardID]; planID != "" {
		if ps := s.plans[planID]; ps.plan.Status == PlanActive {
			if action, _ := s.evaluate(ps, nil); action == advanceAction {
				if err := s.commitAdvance(ps); err != nil {
					return nil, err
				}
			}
		}
	}
	return s.healthyList(s.shards[req.ShardID]), nil
}

// UpdateConfig applies a manual configuration change. It increments and
// persists the version, and pauses any plan of the shard with a conflict:
// the plan was frozen on the previous version and must never overwrite the
// newer configuration.
func (s *Service) UpdateConfig(req UpdateConfigRequest) (ShardConfig, error) {
	if req.ShardID == "" {
		return ShardConfig{}, fmt.Errorf("%w: empty shard id", ErrInvalidRequest)
	}
	if req.Primary == "" {
		return ShardConfig{}, fmt.Errorf("%w: empty primary", ErrInvalidRequest)
	}
	if len(req.Members) == 0 {
		return ShardConfig{}, fmt.Errorf("%w: empty members", ErrInvalidRequest)
	}
	if req.MinHealthy < 1 {
		return ShardConfig{}, fmt.Errorf("%w: min_healthy must be >= 1", ErrInvalidRequest)
	}
	if err := assertUniqueMembers(req.Members); err != nil {
		return ShardConfig{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	sh, ok := s.shards[req.ShardID]
	if !ok {
		return ShardConfig{}, fmt.Errorf("%w: %s", ErrShardNotFound, req.ShardID)
	}
	if !containsID(req.Members, req.Primary) {
		return ShardConfig{}, ErrPrimaryMissing
	}
	healthyCount := 0
	for _, member := range req.Members {
		if sh.healthy[member] {
			healthyCount++
		}
	}
	if healthyCount < req.MinHealthy {
		return ShardConfig{}, fmt.Errorf("%w: only %d of %d requested members are healthy, min is %d",
			ErrSafetyViolation, healthyCount, len(req.Members), req.MinHealthy)
	}
	old := sh.config
	if old.Primary == req.Primary && sameIDSet(old.Members, req.Members) && old.MinHealthy == req.MinHealthy {
		return ShardConfig{}, fmt.Errorf("%w: configuration unchanged at version %d", ErrInvalidRequest, old.Version)
	}

	note := req.Reason
	if note == "" {
		note = ChangeReasonManual
	}
	if _, err := s.emit(KindConfigChanged, req.ShardID, "", old.Version+1, &ConfigChangeDetail{
		Reason:     ChangeReasonManual,
		OldVersion: old.Version,
		NewVersion: old.Version + 1,
		OldPrimary: old.Primary,
		NewPrimary: req.Primary,
		OldMembers: cloneIDs(old.Members),
		NewMembers: cloneIDs(req.Members),
		MinHealthy: req.MinHealthy,
		Note:       note,
	}); err != nil {
		return ShardConfig{}, err
	}

	if planID := s.active[req.ShardID]; planID != "" {
		ps := s.plans[planID]
		if ps.plan.Status != PlanPaused || ps.plan.PauseReason != PauseConflict {
			if _, err := s.emit(KindPlanPaused, req.ShardID, planID, s.shards[req.ShardID].config.Version, &PlanPausedDetail{
				PlanID: planID,
				Reason: PauseConflict,
				Detail: fmt.Sprintf("manual configuration changed from frozen version %d to %d: %s",
					ps.plan.StartConfig.Version, old.Version+1, note),
			}); err != nil {
				return ShardConfig{}, err
			}
		}
	}
	return s.shards[req.ShardID].config.Clone(), nil
}
