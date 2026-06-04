// Package controller implements the Redis replication reconciliation loop.
// On each tick it probes every running Redis pod, identifies the master,
// handles promotion/failover, and keeps pod labels in sync so the
// redis-write Service always routes to the current master.
package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/erdo-enes/redis-replication-controller/internal/config"
	kube "github.com/erdo-enes/redis-replication-controller/internal/kubernetes"
	"github.com/erdo-enes/redis-replication-controller/internal/redis"
)

// Controller runs the reconciliation loop.
type Controller struct {
	cfg    *config.Config
	kube   *kube.Client
	dialer redis.Dialer
	log    *slog.Logger

	masterMissingAt time.Time // zero when a master is present
	failoverEpoch   int       // incremented on each promotion
}

// New creates a Controller.
func New(cfg *config.Config, kube *kube.Client, dialer redis.Dialer, log *slog.Logger) *Controller {
	return &Controller{cfg: cfg, kube: kube, dialer: dialer, log: log}
}

// Run starts the reconciliation loop and blocks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.reconcile(ctx); err != nil {
				c.log.Error("reconcile error", "error", err)
			}
		}
	}
}

type podInfo struct {
	pod  kube.Pod
	info *redis.ReplicationInfo
}

func (c *Controller) reconcile(ctx context.Context) error {
	pods, err := c.kube.ListRedisPods(ctx, c.cfg.RedisPodLabelSelector)
	if err != nil {
		return fmt.Errorf("list pods: %w", err)
	}

	// Probe every Running pod (ready or not — role is independent of readiness).
	var reached []podInfo
	for _, pod := range pods {
		if pod.IP == "" || pod.Phase != "Running" {
			continue
		}
		addr := fmt.Sprintf("%s:%d", pod.IP, c.cfg.RedisPort)
		conn, err := c.dialer.Dial(ctx, addr)
		if err != nil {
			c.log.Warn("cannot connect to pod", "pod", pod.Name, "error", err)
			continue
		}
		info, err := conn.InfoReplication(ctx)
		conn.Close()
		if err != nil {
			c.log.Warn("cannot get replication info", "pod", pod.Name, "error", err)
			continue
		}
		reached = append(reached, podInfo{pod, info})
	}

	var masters, replicas []podInfo
	for _, pi := range reached {
		if pi.info.IsMaster() {
			masters = append(masters, pi)
		} else {
			replicas = append(replicas, pi)
		}
	}

	c.log.Info("reconcile", "total", len(pods), "reached", len(reached),
		"masters", len(masters), "replicas", len(replicas))

	var master *podInfo

	switch {
	case len(masters) == 1:
		c.masterMissingAt = time.Time{}
		m := masters[0]
		master = &m

	case len(masters) > 1:
		// Split-brain: keep the already-labeled master (or highest-offset) and demote the rest.
		c.masterMissingAt = time.Time{}
		picked := pickLabeledOrBest(masters)
		master = &picked
		for i := range masters {
			if masters[i].pod.Name == master.pod.Name {
				continue
			}
			c.log.Warn("split-brain: demoting extra master", "pod", masters[i].pod.Name, "master", master.pod.Name)
			if err := c.replicateOf(ctx, masters[i].pod, master.pod.IP); err != nil {
				c.log.Error("demote failed", "pod", masters[i].pod.Name, "error", err)
			}
		}

	default:
		// No master found.
		if c.masterMissingAt.IsZero() {
			c.masterMissingAt = time.Now()
		}
		elapsed := time.Since(c.masterMissingAt)
		if elapsed < c.cfg.MasterFailureThreshold {
			c.log.Warn("no master detected, waiting for failure threshold",
				"elapsed", elapsed.Round(time.Second),
				"threshold", c.cfg.MasterFailureThreshold)
			return nil
		}
		c.log.Warn("master absent beyond threshold, initiating failover",
			"elapsed", elapsed.Round(time.Second))
		promoted, err := c.failover(ctx, replicas)
		if err != nil {
			return err
		}
		if promoted == nil {
			return nil
		}
		c.masterMissingAt = time.Time{}
		master = promoted
	}

	return c.syncLabels(ctx, pods, master.pod.Name)
}

// syncLabels ensures every pod has the correct role label.
func (c *Controller) syncLabels(ctx context.Context, pods []kube.Pod, masterName string) error {
	for _, pod := range pods {
		want := kube.RoleReplica
		if pod.Name == masterName {
			want = kube.RoleMaster
		}
		if pod.RoleLabel() == want {
			continue
		}
		c.log.Info("updating role label", "pod", pod.Name, "role", want)
		if err := c.kube.SetRoleLabel(ctx, pod.Name, want); err != nil {
			c.log.Error("failed to set role label", "pod", pod.Name, "error", err)
		}
	}
	return nil
}

// failover promotes the best available replica and redirects the others.
func (c *Controller) failover(ctx context.Context, replicas []podInfo) (*podInfo, error) {
	candidate := c.selectCandidate(replicas)
	if candidate == nil {
		c.log.Error("no healthy replica available for promotion")
		return nil, nil
	}

	c.failoverEpoch++
	c.log.Info("promoting replica to master", "pod", candidate.pod.Name, "epoch", c.failoverEpoch)

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
			c.log.Warn("CONFIG REWRITE failed on promoted pod", "pod", candidate.pod.Name, "error", err)
		}
	}

	annotations := map[string]string{
		config.AnnotationFailoverEpoch: fmt.Sprintf("%d", c.failoverEpoch),
		config.AnnotationPromotedAt:    time.Now().UTC().Format(time.RFC3339),
	}
	if err := c.kube.SetAnnotations(ctx, candidate.pod.Name, annotations); err != nil {
		c.log.Warn("failed to set failover annotations", "pod", candidate.pod.Name, "error", err)
	}

	for _, r := range replicas {
		if r.pod.Name == candidate.pod.Name {
			continue
		}
		if err := c.replicateOf(ctx, r.pod, candidate.pod.IP); err != nil {
			c.log.Warn("failed to redirect replica to new master",
				"pod", r.pod.Name, "master", candidate.pod.Name, "error", err)
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

// selectCandidate picks the best replica to promote based on the configured strategy.
func (c *Controller) selectCandidate(replicas []podInfo) *podInfo {
	if len(replicas) == 0 {
		return nil
	}

	switch c.cfg.InitialMasterStrategy {
	case config.StrategyAnnotationPreferred:
		for i := range replicas {
			if replicas[i].pod.Annotations[config.AnnotationPreferredMaster] == "true" {
				r := replicas[i]
				return &r
			}
		}
		// No annotation found — fall through to first in list.
		r := replicas[0]
		return &r

	case config.StrategyLowestPodOrdinal:
		best := replicas[0]
		for i := range replicas {
			if replicas[i].pod.Ordinal >= 0 &&
				(best.pod.Ordinal < 0 || replicas[i].pod.Ordinal < best.pod.Ordinal) {
				best = replicas[i]
			}
		}
		return &best

	case config.StrategyFirstHealthy:
		r := replicas[0]
		return &r
	}

	// Default fallback: highest replication offset (most up-to-date replica).
	best := replicas[0]
	for i := range replicas {
		if replicas[i].info.Offset() > best.info.Offset() {
			best = replicas[i]
		}
	}
	return &best
}

// pickLabeledOrBest returns the already-labeled master from a split-brain set,
// or the one with the highest replication offset if none is labeled.
func pickLabeledOrBest(masters []podInfo) podInfo {
	for _, m := range masters {
		if m.pod.HasMasterLabel() {
			return m
		}
	}
	best := masters[0]
	for i := range masters {
		if masters[i].info.Offset() > best.info.Offset() {
			best = masters[i]
		}
	}
	return best
}
