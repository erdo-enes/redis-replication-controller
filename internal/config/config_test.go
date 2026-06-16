package config

import (
	"testing"
	"time"
)

var allEnvKeys = []string{
	"REDIS_NAMESPACE", "REDIS_POD_LABEL_SELECTOR", "REDIS_PORT", "REDIS_WRITE_SERVICE_NAME",
	"RECONCILE_INTERVAL_SECONDS", "MASTER_FAILURE_THRESHOLD_SECONDS", "REDIS_CONNECT_TIMEOUT_SECONDS",
	"REDIS_COMMAND_TIMEOUT_SECONDS", "CONTROLLER_ID", "ENABLE_LEADER_ELECTION", "LEASE_NAME",
	"LEASE_NAMESPACE", "INITIAL_MASTER_STRATEGY", "ENABLE_CONFIG_REWRITE", "HEALTH_PROBE_ADDR",
	"REDIS_SET_LABEL_KEY", "DEFAULT_SET_NAME", "PROBE_CONCURRENCY", "REDIS_NAMESPACES",
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range allEnvKeys {
		t.Setenv(k, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if c.RedisPort != 6379 {
		t.Errorf("RedisPort = %d, want 6379", c.RedisPort)
	}
	if c.RedisPodLabelSelector != "app=redis" {
		t.Errorf("RedisPodLabelSelector = %q", c.RedisPodLabelSelector)
	}
	if c.RedisWriteServiceName != "redis-write" {
		t.Errorf("RedisWriteServiceName = %q", c.RedisWriteServiceName)
	}
	if c.ReconcileInterval != 10*time.Second {
		t.Errorf("ReconcileInterval = %v", c.ReconcileInterval)
	}
	if c.MasterFailureThreshold != 15*time.Second {
		t.Errorf("MasterFailureThreshold = %v", c.MasterFailureThreshold)
	}
	if !c.EnableLeaderElection {
		t.Errorf("EnableLeaderElection = false, want true")
	}
	if c.InitialMasterStrategy != StrategyLowestPodOrdinal {
		t.Errorf("InitialMasterStrategy = %q", c.InitialMasterStrategy)
	}
	if c.ConfigRewrite {
		t.Errorf("ConfigRewrite = true, want false")
	}
	if c.LeaseNamespace != c.RedisNamespace {
		t.Errorf("LeaseNamespace = %q, want = RedisNamespace %q", c.LeaseNamespace, c.RedisNamespace)
	}
	if c.RedisSetLabelKey != "redis-set" {
		t.Errorf("RedisSetLabelKey = %q, want redis-set", c.RedisSetLabelKey)
	}
	if c.DefaultSetName != "default" {
		t.Errorf("DefaultSetName = %q, want default", c.DefaultSetName)
	}
	if c.ProbeConcurrency != 16 {
		t.Errorf("ProbeConcurrency = %d, want 16", c.ProbeConcurrency)
	}
	if len(c.RedisNamespaces) != 1 || c.RedisNamespaces[0] != c.RedisNamespace {
		t.Errorf("RedisNamespaces = %v, want [%s]", c.RedisNamespaces, c.RedisNamespace)
	}
}

func TestLoadNamespacesList(t *testing.T) {
	clearEnv(t)
	t.Setenv("REDIS_NAMESPACES", " team-a, team-b ,team-a,, team-c ")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	want := []string{"team-a", "team-b", "team-c"} // trimmed, de-duped, ordered
	if len(c.RedisNamespaces) != len(want) {
		t.Fatalf("RedisNamespaces = %v, want %v", c.RedisNamespaces, want)
	}
	for i, ns := range want {
		if c.RedisNamespaces[i] != ns {
			t.Errorf("RedisNamespaces[%d] = %q, want %q", i, c.RedisNamespaces[i], ns)
		}
	}
}

func TestLoadOverrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("REDIS_NAMESPACE", "prod")
	t.Setenv("REDIS_PORT", "6380")
	t.Setenv("RECONCILE_INTERVAL_SECONDS", "5")
	t.Setenv("MASTER_FAILURE_THRESHOLD_SECONDS", "30")
	t.Setenv("ENABLE_LEADER_ELECTION", "false")
	t.Setenv("ENABLE_CONFIG_REWRITE", "true")
	t.Setenv("INITIAL_MASTER_STRATEGY", "FIRST-HEALTHY") // case-insensitive
	t.Setenv("LEASE_NAMESPACE", "kube-system")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if c.RedisNamespace != "prod" || c.RedisPort != 6380 {
		t.Errorf("namespace/port = %q/%d", c.RedisNamespace, c.RedisPort)
	}
	if c.ReconcileInterval != 5*time.Second || c.MasterFailureThreshold != 30*time.Second {
		t.Errorf("durations = %v / %v", c.ReconcileInterval, c.MasterFailureThreshold)
	}
	if c.EnableLeaderElection {
		t.Errorf("EnableLeaderElection = true, want false")
	}
	if !c.ConfigRewrite {
		t.Errorf("ConfigRewrite = false, want true")
	}
	if c.InitialMasterStrategy != StrategyFirstHealthy {
		t.Errorf("InitialMasterStrategy = %q", c.InitialMasterStrategy)
	}
	if c.LeaseNamespace != "kube-system" {
		t.Errorf("LeaseNamespace = %q", c.LeaseNamespace)
	}
}

func TestLoadValidationErrors(t *testing.T) {
	cases := map[string]map[string]string{
		"bad strategy": {"INITIAL_MASTER_STRATEGY": "best-guess"},
		"bad port int": {"REDIS_PORT": "abc"},
		"port range":   {"REDIS_PORT": "70000"},
		"bad bool":     {"ENABLE_LEADER_ELECTION": "maybe"},
		"bad seconds":  {"RECONCILE_INTERVAL_SECONDS": "-1"},
		"bad probe":    {"PROBE_CONCURRENCY": "0"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range env {
				t.Setenv(k, v)
			}
			if _, err := Load(); err == nil {
				t.Fatalf("expected error for %s, got nil", name)
			}
		})
	}
}
