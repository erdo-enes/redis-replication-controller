// Command redis-replication-controller runs the Redis replication/failover
// reconciliation loop inside Kubernetes using in-cluster configuration.
package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientgo "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/erdo-enes/redis-replication-controller/internal/config"
	"github.com/erdo-enes/redis-replication-controller/internal/controller"
	kube "github.com/erdo-enes/redis-replication-controller/internal/kubernetes"
	"github.com/erdo-enes/redis-replication-controller/internal/leader"
	"github.com/erdo-enes/redis-replication-controller/internal/redis"
)

func main() {
	logger := newLogger()

	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	logger.Info("controller started",
		"controllerID", cfg.ControllerID,
		"namespace", cfg.RedisNamespace,
		"selector", cfg.RedisPodLabelSelector,
		"setLabelKey", cfg.RedisSetLabelKey,
		"defaultSetName", cfg.DefaultSetName,
		"probeConcurrency", cfg.ProbeConcurrency,
		"redisPort", cfg.RedisPort,
		"writeService", cfg.RedisWriteServiceName,
		"reconcileIntervalSeconds", cfg.ReconcileInterval.Seconds(),
		"failureThresholdSeconds", cfg.MasterFailureThreshold.Seconds(),
		"initialMasterStrategy", cfg.InitialMasterStrategy,
		"leaderElection", cfg.EnableLeaderElection,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("failed to load in-cluster kubernetes config", "error", err)
		os.Exit(1)
	}
	cs, err := clientgo.NewForConfig(restCfg)
	if err != nil {
		logger.Error("failed to build kubernetes client", "error", err)
		os.Exit(1)
	}

	ctrl := controller.New(
		cfg,
		kube.New(cs, cfg.RedisNamespace),
		redis.NewDialer(cfg.RedisConnectTimeout, cfg.RedisCommandTimeout),
		logger,
	)

	go serveHealth(ctx, cfg.HealthProbeAddr, ctrl.Ready, logger)

	run := func(c context.Context) {
		if err := ctrl.Run(c); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("controller stopped with error", "error", err)
		}
	}

	if cfg.EnableLeaderElection {
		leader.Run(ctx, cs, cfg, logger, run)
	} else {
		logger.Warn("leader election disabled; assuming a single active controller replica")
		run(ctx)
	}

	logger.Info("controller shutdown complete")
}

func newLogger() *slog.Logger {
	level := slog.LevelInfo
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		_ = level.UnmarshalText([]byte(v))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

// serveHealth exposes /healthz and /readyz for Kubernetes probes. /healthz is a
// plain liveness signal (the process is up); /readyz reflects whether the
// reconcile loop is actually making progress, via ready.
func serveHealth(ctx context.Context, addr string, ready func() bool, logger *slog.Logger) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "reconcile stale\n")
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Info("health probe server listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("health probe server error", "error", err)
	}
}
