// Package controller implements the Redis replication reconciliation loop.
// On each tick it groups the discovered Redis pods into independent replication
// sets (by a configurable label), then for every set probes its pods in
// parallel, identifies the master, handles promotion/failover, and keeps pod
// labels in sync so each set's redis-write Service routes to its current master.
//
// A single controller manages any number of sets. Each set keeps its own
// failure timer and failover epoch, so a healthy set can never delay or mask a
// failover in another set.
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/erdo-enes/redis-replication-controller/internal/config"
	kube "github.com/erdo-enes/redis-replication-controller/internal/kubernetes"
	"github.com/erdo-enes/redis-replication-controller/internal/redis"
)

// setState is the per-replication-set bookkeeping the reconcile loop carries
// between ticks.
type setState struct {
	masterMissingAt time.Time // zero when a master is present
	failoverEpoch   int       // last epoch this process applied for the set
}

// Controller runs the reconciliation loop.
type Controller struct {
	cfg    *config.Config
	kube   *kube.Client
	dialer redis.Dialer
	log    *slog.Logger

	// sets holds per-set state, keyed by set name. It is only touched from the
	// single reconcile goroutine.
	sets map[string]*setState

	// mu guards the readiness fields, which are read from the health server
	// goroutine.
	mu              sync.Mutex
	started         bool
	lastReconcileAt time.Time
}

// New creates a Controller.
func New(cfg *config.Config, kube *kube.Client, dialer redis.Dialer, log *slog.Logger) *Controller {
	return &Controller{
		cfg:    cfg,
		kube:   kube,
		dialer: dialer,
		log:    log,
		sets:   make(map[string]*setState),
	}
}

// Run starts the reconciliation loop and blocks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) error {
	c.mu.Lock()
	c.started = true
	c.lastReconcileAt = time.Now() // fresh during the first interval
	c.mu.Unlock()

	ticker := time.NewTicker(c.cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.reconcile(ctx); err != nil {
				c.log.Error("reconcile error", "error", err)
				continue
			}
			c.mu.Lock()
			c.lastReconcileAt = time.Now()
			c.mu.Unlock()
		}
	}
}

// Ready reports whether the controller is healthy enough to serve /readyz. A
// replica that has not started leading yet (standby) is considered ready; an
// active leader is ready only while its reconcile loop keeps succeeding.
func (c *Controller) Ready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started {
		return true
	}
	return time.Since(c.lastReconcileAt) < 3*c.cfg.ReconcileInterval
}

type podInfo struct {
	pod  kube.Pod
	info *redis.ReplicationInfo
}

// reconcile lists all managed Redis pods, splits them into independent
// replication sets, and reconciles each set against its own state.
func (c *Controller) reconcile(ctx context.Context) error {
	pods, err := c.kube.ListRedisPods(ctx, c.cfg.RedisPodLabelSelector)
	if err != nil {
		return fmt.Errorf("list pods: %w", err)
	}

	groups := groupBySet(pods, c.cfg.RedisSetLabelKey, c.cfg.DefaultSetName)
	for _, name := range sortedKeys(groups) {
		st := c.stateFor(name)
		if err := c.reconcileSet(ctx, name, groups[name], st); err != nil {
			// A single set's failure must not stop the others or flap the
			// controller's readiness; log and continue.
			c.log.Error("reconcile set error", "set", name, "error", err)
		}
	}
	c.pruneVanishedSets(groups)
	return nil
}

// reconcileSet runs the master-election/failover logic for one replication set.
func (c *Controller) reconcileSet(ctx context.Context, setName string, pods []kube.Pod, st *setState) error {
	reached := c.probe(ctx, setName, pods)

	var masters, replicas []podInfo
	for _, pi := range reached {
		if pi.info.IsMaster() {
			masters = append(masters, pi)
		} else {
			replicas = append(replicas, pi)
		}
	}

	c.log.Info("reconcile", "set", setName, "total", len(pods), "reached", len(reached),
		"masters", len(masters), "replicas", len(replicas))

	var master *podInfo

	switch {
	case len(masters) == 1:
		st.masterMissingAt = time.Time{}
		m := masters[0]
		master = &m

	case len(masters) > 1:
		st.masterMissingAt = time.Time{}
		var picked podInfo
		if m, ok := labeledMaster(masters); ok {
			// A pod already carries the authoritative master label (e.g. from a
			// completed failover). Always keep it and demote the rest — never
			// fight a promotion the controller already made.
			picked = m
			c.log.Warn("multiple masters detected; converging on labeled master", "set", setName, "master", picked.pod.Name)
		} else {
			// No pod is labeled master: this is a fresh set where every pod
			// booted as its own standalone master. Choose the initial master
			// deterministically via INITIAL_MASTER_STRATEGY (default
			// lowest-pod-ordinal => redis-0) so bootstrap is predictable.
			picked = *c.selectBootstrapCandidate(masters)
			c.log.Info("bootstrapping initial master from standalone pods",
				"set", setName, "master", picked.pod.Name, "strategy", c.cfg.InitialMasterStrategy)
		}
		master = &picked
		for i := range masters {
			if masters[i].pod.Name == master.pod.Name {
				continue
			}
			c.log.Warn("demoting extra master", "set", setName, "pod", masters[i].pod.Name, "master", master.pod.Name)
			if err := c.replicateOf(ctx, masters[i].pod, master.pod.IP); err != nil {
				c.log.Error("demote failed", "set", setName, "pod", masters[i].pod.Name, "error", err)
			}
		}

	default:
		// No master found.
		if st.masterMissingAt.IsZero() {
			st.masterMissingAt = time.Now()
		}
		elapsed := time.Since(st.masterMissingAt)
		if elapsed < c.cfg.MasterFailureThreshold {
			c.log.Warn("no master detected, waiting for failure threshold",
				"set", setName, "elapsed", elapsed.Round(time.Second),
				"threshold", c.cfg.MasterFailureThreshold)
			return nil
		}
		c.log.Warn("master absent beyond threshold, initiating failover",
			"set", setName, "elapsed", elapsed.Round(time.Second))
		promoted, err := c.failover(ctx, setName, pods, replicas, st)
		if err != nil {
			return err
		}
		if promoted == nil {
			return nil
		}
		st.masterMissingAt = time.Time{}
		master = promoted
	}

	return c.syncLabels(ctx, setName, pods, master.pod.Name)
}

// probe connects to every Running pod in the set concurrently and returns the
// ones that answered INFO replication. Bounded concurrency keeps one set's
// unreachable pods from stalling the reconcile of other sets.
func (c *Controller) probe(ctx context.Context, setName string, pods []kube.Pod) []podInfo {
	type slot struct {
		pi podInfo
		ok bool
	}
	results := make([]slot, len(pods))

	limit := c.cfg.ProbeConcurrency
	if limit < 1 {
		limit = 1
	}
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup

	for i := range pods {
		pod := pods[i]
		if pod.IP == "" || pod.Phase != "Running" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, pod kube.Pod) {
			defer wg.Done()
			defer func() { <-sem }()

			addr := fmt.Sprintf("%s:%d", pod.IP, c.cfg.RedisPort)
			conn, err := c.dialer.Dial(ctx, addr)
			if err != nil {
				c.log.Warn("cannot connect to pod", "set", setName, "pod", pod.Name, "error", err)
				return
			}
			info, err := conn.InfoReplication(ctx)
			conn.Close()
			if err != nil {
				c.log.Warn("cannot get replication info", "set", setName, "pod", pod.Name, "error", err)
				return
			}
			results[idx] = slot{podInfo{pod, info}, true}
		}(i, pod)
	}
	wg.Wait()

	reached := make([]podInfo, 0, len(results))
	for _, r := range results {
		if r.ok {
			reached = append(reached, r.pi)
		}
	}
	return reached
}

// syncLabels ensures every pod in the set has the correct role label.
func (c *Controller) syncLabels(ctx context.Context, setName string, pods []kube.Pod, masterName string) error {
	for _, pod := range pods {
		want := kube.RoleReplica
		if pod.Name == masterName {
			want = kube.RoleMaster
		}
		if pod.RoleLabel() == want {
			continue
		}
		c.log.Info("updating role label", "set", setName, "pod", pod.Name, "role", want)
		if err := c.kube.SetRoleLabel(ctx, pod.Name, want); err != nil {
			c.log.Error("failed to set role label", "set", setName, "pod", pod.Name, "error", err)
		}
	}
	return nil
}

// failover promotes the best available replica and redirects the others.
func (c *Controller) failover(ctx context.Context, setName string, pods []kube.Pod, replicas []podInfo, st *setState) (*podInfo, error) {
	candidate := selectFailoverCandidate(replicas)
	if candidate == nil {
		c.log.Error("no healthy replica available for promotion", "set", setName)
		return nil, nil
	}

	// Derive the next epoch from what is recorded on the pods so a controller
	// restart or leader handover continues the sequence instead of resetting to
	// zero and writing a smaller epoch over a larger one.
	epoch := maxEpoch(pods) + 1
	if st.failoverEpoch >= epoch {
		epoch = st.failoverEpoch + 1
	}
	st.failoverEpoch = epoch
	c.log.Info("promoting replica to master", "set", setName, "pod", candidate.pod.Name,
		"epoch", epoch, "offset", candidate.info.Offset())

	addr := fmt.Sprintf("%s:%d", candidate.pod.IP, c.cfg.RedisPort)
	conn, err := c.dialer.Dial(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("connect to candidate %s: %w", candidate.pod.Name, err)
	}
	defer conn.Close()

	if err := conn.ReplicaOfNoOne(ctx); err != nil {
		return nil, fmt.Errorf("REPLICAOF NO ONE on %s: %w", candidate.pod.Name, err)
	}
	if c.cfg.ConfigRewrite {
		if err := conn.ConfigRewrite(ctx); err != nil {
			c.log.Warn("CONFIG REWRITE failed on promoted pod", "set", setName, "pod", candidate.pod.Name, "error", err)
		}
	}

	annotations := map[string]string{
		config.AnnotationFailoverEpoch: strconv.Itoa(epoch),
		config.AnnotationPromotedAt:    time.Now().UTC().Format(time.RFC3339),
	}
	if err := c.kube.SetAnnotations(ctx, candidate.pod.Name, annotations); err != nil {
		c.log.Warn("failed to set failover annotations", "set", setName, "pod", candidate.pod.Name, "error", err)
	}

	for _, r := range replicas {
		if r.pod.Name == candidate.pod.Name {
			continue
		}
		if err := c.replicateOf(ctx, r.pod, candidate.pod.IP); err != nil {
			c.log.Warn("failed to redirect replica to new master",
				"set", setName, "pod", r.pod.Name, "master", candidate.pod.Name, "error", err)
		}
	}

	return candidate, nil
}

// replicateOf sends REPLICAOF <masterIP> <port> to a pod.
func (c *Controller) replicateOf(ctx context.Context, pod kube.Pod, masterIP string) error {
	addr := fmt.Sprintf("%s:%d", pod.IP, c.cfg.RedisPort)
	conn, err := c.dialer.Dial(ctx, addr)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", pod.Name, err)
	}
	defer conn.Close()
	if err := conn.ReplicaOf(ctx, masterIP, c.cfg.RedisPort); err != nil {
		return fmt.Errorf("REPLICAOF on %s: %w", pod.Name, err)
	}
	if c.cfg.ConfigRewrite {
		if err := conn.ConfigRewrite(ctx); err != nil {
			c.log.Warn("CONFIG REWRITE failed", "pod", pod.Name, "error", err)
		}
	}
	return nil
}

// selectBootstrapCandidate picks the initial master from a set of standalone
// pods according to the configured strategy. It is used only at bootstrap, when
// no pod is labeled master yet, so a predictable, stable choice matters more
// than replication offset.
func (c *Controller) selectBootstrapCandidate(candidates []podInfo) *podInfo {
	if len(candidates) == 0 {
		return nil
	}

	switch c.cfg.InitialMasterStrategy {
	case config.StrategyAnnotationPreferred:
		for i := range candidates {
			if candidates[i].pod.Annotations[config.AnnotationPreferredMaster] == "true" {
				r := candidates[i]
				return &r
			}
		}
		// No annotation found — fall through to first in list.
		r := candidates[0]
		return &r

	case config.StrategyLowestPodOrdinal:
		best := candidates[0]
		for i := range candidates {
			if candidates[i].pod.Ordinal >= 0 &&
				(best.pod.Ordinal < 0 || candidates[i].pod.Ordinal < best.pod.Ordinal) {
				best = candidates[i]
			}
		}
		return &best

	case config.StrategyFirstHealthy:
		r := candidates[0]
		return &r
	}

	// Default fallback: highest replication offset (most up-to-date pod).
	best := candidates[0]
	for i := range candidates {
		if candidates[i].info.Offset() > best.info.Offset() {
			best = candidates[i]
		}
	}
	return &best
}

// selectFailoverCandidate picks which replica to promote during a failover.
// Unlike bootstrap, data safety dominates here: prefer replicas whose link to
// the (now-gone) master was still healthy, then the highest replication offset
// to minimise lost writes, breaking ties by lowest pod ordinal for determinism.
func selectFailoverCandidate(replicas []podInfo) *podInfo {
	if len(replicas) == 0 {
		return nil
	}

	candidates := replicas
	var linkUp []podInfo
	for _, r := range replicas {
		// Empty status means the field was absent (e.g. INFO from a node that
		// just lost its master); treat only an explicit "down" as unhealthy.
		if r.info.MasterLinkStatus != "down" {
			linkUp = append(linkUp, r)
		}
	}
	if len(linkUp) > 0 {
		candidates = linkUp
	}

	best := candidates[0]
	for i := range candidates {
		r := candidates[i]
		switch {
		case r.info.Offset() > best.info.Offset():
			best = r
		case r.info.Offset() == best.info.Offset() && betterOrdinal(r.pod.Ordinal, best.pod.Ordinal):
			best = r
		}
	}
	return &best
}

// betterOrdinal reports whether ordinal a is the more preferred (lower, valid)
// of the two; unknown ordinals (-1) are least preferred.
func betterOrdinal(a, b int) bool {
	switch {
	case a < 0:
		return false
	case b < 0:
		return true
	default:
		return a < b
	}
}

// labeledMaster returns the first master that carries the authoritative
// redis-current-role=master label, if any does.
func labeledMaster(masters []podInfo) (podInfo, bool) {
	for _, m := range masters {
		if m.pod.HasMasterLabel() {
			return m, true
		}
	}
	return podInfo{}, false
}

// groupBySet splits pods into independent replication sets keyed by the value
// of the set label; pods without the label fall into def. Each group keeps the
// caller's incoming order (ListRedisPods sorts by ordinal), so per-set ordinal
// selection stays deterministic.
func groupBySet(pods []kube.Pod, key, def string) map[string][]kube.Pod {
	groups := make(map[string][]kube.Pod)
	for _, p := range pods {
		name := p.SetName(key, def)
		groups[name] = append(groups[name], p)
	}
	return groups
}

// maxEpoch returns the largest failover epoch recorded on any pod in the set,
// or 0 when none carry the annotation.
func maxEpoch(pods []kube.Pod) int {
	max := 0
	for _, p := range pods {
		v := p.Annotations[config.AnnotationFailoverEpoch]
		if v == "" {
			continue
		}
		if n, err := strconv.Atoi(v); err == nil && n > max {
			max = n
		}
	}
	return max
}

// stateFor returns the (lazily created) state for a set.
func (c *Controller) stateFor(name string) *setState {
	st := c.sets[name]
	if st == nil {
		st = &setState{}
		c.sets[name] = st
	}
	return st
}

// pruneVanishedSets drops state for sets that no longer have any pods so the
// map does not grow without bound as sets are added and removed.
func (c *Controller) pruneVanishedSets(groups map[string][]kube.Pod) {
	for name := range c.sets {
		if _, ok := groups[name]; !ok {
			delete(c.sets, name)
		}
	}
}

// sortedKeys returns the group names in a stable order for deterministic logs
// and reconciliation order.
func sortedKeys(groups map[string][]kube.Pod) []string {
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
