// Package config loads and validates the controller configuration from the
// environment. All tunables are supplied as environment variables so the
// controller can be configured purely through its Kubernetes Deployment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Label and annotation keys owned by the controller.
const (
	// AnnotationPreferredMaster, when set to "true" on a Redis Pod, marks it as
	// the preferred initial master for the "annotation-preferred" strategy.
	AnnotationPreferredMaster = "redis-controller/preferred-master"
	// AnnotationFailoverEpoch records the monotonically increasing failover
	// generation on the Pod that was most recently promoted to master.
	AnnotationFailoverEpoch = "redis-controller/failover-epoch"
	// AnnotationPromotedAt records the RFC3339 timestamp of the last promotion.
	AnnotationPromotedAt = "redis-controller/promoted-at"
)

// Initial master selection strategies (INITIAL_MASTER_STRATEGY).
const (
	StrategyFirstHealthy        = "first-healthy"
	StrategyLowestPodOrdinal    = "lowest-pod-ordinal"
	StrategyAnnotationPreferred = "annotation-preferred"
)

// Config is the fully resolved controller configuration.
type Config struct {
	RedisNamespace         string
	RedisPodLabelSelector  string
	RedisPort              int
	RedisWriteServiceName  string
	ReconcileInterval      time.Duration
	MasterFailureThreshold time.Duration
	RedisConnectTimeout    time.Duration
	RedisCommandTimeout    time.Duration
	ControllerID           string
	EnableLeaderElection   bool
	LeaseName              string
	LeaseNamespace         string
	InitialMasterStrategy  string
	// ConfigRewrite controls whether CONFIG REWRITE is issued after changing a
	// node's replication role so the change survives a Redis restart. Optional;
	// disabled by default (ENABLE_CONFIG_REWRITE).
	ConfigRewrite bool
	// HealthProbeAddr is the listen address for the /healthz and /readyz probes.
	HealthProbeAddr string
}

// Load reads the configuration from the environment, applying defaults and
// validating the result.
func Load() (*Config, error) {
	c := &Config{
		RedisNamespace:        getEnv("REDIS_NAMESPACE", "default"),
		RedisPodLabelSelector: getEnv("REDIS_POD_LABEL_SELECTOR", "app=redis"),
		RedisWriteServiceName: getEnv("REDIS_WRITE_SERVICE_NAME", "redis-write"),
		ControllerID:          getEnv("CONTROLLER_ID", defaultControllerID()),
		LeaseName:             getEnv("LEASE_NAME", "redis-replication-controller"),
		InitialMasterStrategy: strings.ToLower(getEnv("INITIAL_MASTER_STRATEGY", StrategyLowestPodOrdinal)),
		HealthProbeAddr:       getEnv("HEALTH_PROBE_ADDR", ":8081"),
	}
	c.LeaseNamespace = getEnv("LEASE_NAMESPACE", c.RedisNamespace)

	var err error
	if c.RedisPort, err = getEnvInt("REDIS_PORT", 6379); err != nil {
		return nil, err
	}
	if c.ReconcileInterval, err = getEnvSeconds("RECONCILE_INTERVAL_SECONDS", 10); err != nil {
		return nil, err
	}
	if c.MasterFailureThreshold, err = getEnvSeconds("MASTER_FAILURE_THRESHOLD_SECONDS", 15); err != nil {
		return nil, err
	}
	if c.RedisConnectTimeout, err = getEnvSeconds("REDIS_CONNECT_TIMEOUT_SECONDS", 2); err != nil {
		return nil, err
	}
	if c.RedisCommandTimeout, err = getEnvSeconds("REDIS_COMMAND_TIMEOUT_SECONDS", 2); err != nil {
		return nil, err
	}
	if c.EnableLeaderElection, err = getEnvBool("ENABLE_LEADER_ELECTION", true); err != nil {
		return nil, err
	}
	if c.ConfigRewrite, err = getEnvBool("ENABLE_CONFIG_REWRITE", false); err != nil {
		return nil, err
	}

	return c, c.validate()
}

func (c *Config) validate() error {
	if c.RedisPort < 1 || c.RedisPort > 65535 {
		return fmt.Errorf("REDIS_PORT out of range: %d", c.RedisPort)
	}
	if strings.TrimSpace(c.RedisPodLabelSelector) == "" {
		return fmt.Errorf("REDIS_POD_LABEL_SELECTOR must not be empty")
	}
	if strings.TrimSpace(c.RedisNamespace) == "" {
		return fmt.Errorf("REDIS_NAMESPACE must not be empty")
	}
	switch c.InitialMasterStrategy {
	case StrategyFirstHealthy, StrategyLowestPodOrdinal, StrategyAnnotationPreferred:
	default:
		return fmt.Errorf("invalid INITIAL_MASTER_STRATEGY %q (allowed: %s, %s, %s)",
			c.InitialMasterStrategy, StrategyFirstHealthy, StrategyLowestPodOrdinal, StrategyAnnotationPreferred)
	}
	if c.ReconcileInterval <= 0 {
		return fmt.Errorf("RECONCILE_INTERVAL_SECONDS must be > 0")
	}
	if c.RedisCommandTimeout <= 0 {
		return fmt.Errorf("REDIS_COMMAND_TIMEOUT_SECONDS must be > 0")
	}
	return nil
}

func defaultControllerID() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "redis-replication-controller"
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0, fmt.Errorf("invalid integer for %s: %q", key, v)
	}
	return n, nil
}

func getEnvSeconds(key string, def int) (time.Duration, error) {
	n, err := getEnvInt(key, def)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("%s must be >= 0", key)
	}
	return time.Duration(n) * time.Second, nil
}

func getEnvBool(key string, def bool) (bool, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		return false, fmt.Errorf("invalid boolean for %s: %q", key, v)
	}
	return b, nil
}
