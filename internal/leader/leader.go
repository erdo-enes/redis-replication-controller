// Package leader wraps client-go's lease-based leader election so that only one
// controller replica performs failover at a time.
package leader

import (
	"context"
	"log/slog"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientgo "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/endo-sys/redis-replication-controller/internal/config"
)

// Run blocks running leader election until ctx is cancelled. onStarted is
// invoked (with a context that is cancelled when leadership is lost) when this
// instance becomes the leader.
func Run(ctx context.Context, cs clientgo.Interface, cfg *config.Config, log *slog.Logger, onStarted func(ctx context.Context)) {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      cfg.LeaseName,
			Namespace: cfg.LeaseNamespace,
		},
		Client: cs.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: cfg.ControllerID,
		},
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(c context.Context) {
				log.Info("leader election acquired", "id", cfg.ControllerID,
					"lease", cfg.LeaseName, "namespace", cfg.LeaseNamespace)
				onStarted(c)
			},
			OnStoppedLeading: func() {
				log.Warn("leader election lost", "id", cfg.ControllerID)
			},
			OnNewLeader: func(identity string) {
				if identity != cfg.ControllerID {
					log.Info("observed leader", "leader", identity)
				}
			},
		},
	})
}
