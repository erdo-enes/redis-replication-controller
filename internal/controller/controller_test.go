package controller

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/erdo-enes/redis-replication-controller/internal/config"
	kube "github.com/erdo-enes/redis-replication-controller/internal/kubernetes"
	"github.com/erdo-enes/redis-replication-controller/internal/redis"
)

// --- fake Redis dialer -------------------------------------------------------

type fakeNode struct {
	role       string // redis.RoleMaster | redis.RoleReplica
	offset     int64
	linkStatus string // "" | "up" | "down"
	masterHost string
}

type opKind int

const (
	opReplicaOf opKind = iota
	opReplicaOfNoOne
)

type op struct {
	addr    string
	kind    opKind
	masterH string // host arg for REPLICAOF
}

type fakeDialer struct {
	mu    sync.Mutex
	nodes map[string]*fakeNode // keyed by "ip:port"
	ops   []op
}

func (d *fakeDialer) Dial(_ context.Context, addr string) (redis.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n, ok := d.nodes[addr]
	if !ok {
		return nil, &net0Error{addr}
	}
	return &fakeConn{d: d, addr: addr, node: n}, nil
}

type net0Error struct{ addr string }

func (e *net0Error) Error() string { return "no node at " + e.addr }

type fakeConn struct {
	d    *fakeDialer
	addr string
	node *fakeNode
}

func (c *fakeConn) Ping(context.Context) error { return nil }
func (c *fakeConn) Role(context.Context) (*redis.RoleInfo, error) {
	return &redis.RoleInfo{Role: c.node.role}, nil
}

func (c *fakeConn) InfoReplication(context.Context) (*redis.ReplicationInfo, error) {
	c.d.mu.Lock()
	defer c.d.mu.Unlock()
	ri := &redis.ReplicationInfo{Role: c.node.role}
	if c.node.role == redis.RoleMaster {
		ri.MasterReplOffset = c.node.offset
	} else {
		ri.SlaveReplOffset = c.node.offset
		ri.MasterHost = c.node.masterHost
		ri.MasterLinkStatus = c.node.linkStatus
	}
	return ri, nil
}

func (c *fakeConn) ReplicaOf(_ context.Context, host string, _ int) error {
	c.d.mu.Lock()
	defer c.d.mu.Unlock()
	c.node.role = redis.RoleReplica
	c.node.masterHost = host
	c.d.ops = append(c.d.ops, op{addr: c.addr, kind: opReplicaOf, masterH: host})
	return nil
}

func (c *fakeConn) ReplicaOfNoOne(context.Context) error {
	c.d.mu.Lock()
	defer c.d.mu.Unlock()
	c.node.role = redis.RoleMaster
	c.node.masterHost = ""
	c.d.ops = append(c.d.ops, op{addr: c.addr, kind: opReplicaOfNoOne})
	return nil
}

func (c *fakeConn) ConfigRewrite(context.Context) error { return nil }
func (c *fakeConn) Close() error                        { return nil }

// --- helpers -----------------------------------------------------------------

const testNS = "redis"

func testPod(name, ip, set, role string, ann map[string]string) *corev1.Pod {
	labels := map[string]string{"app": "redis", "redis-set": set}
	if role != "" {
		labels[kube.LabelRole] = role
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: labels, Annotations: ann},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			PodIP:      ip,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func testConfig() *config.Config {
	return &config.Config{
		RedisPodLabelSelector:  "app=redis",
		RedisPort:              6379,
		ReconcileInterval:      10 * time.Second,
		MasterFailureThreshold: 0, // fail over immediately in tests
		InitialMasterStrategy:  config.StrategyLowestPodOrdinal,
		RedisSetLabelKey:       "redis-set",
		DefaultSetName:         "default",
		ProbeConcurrency:       4,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func addr(ip string) string { return ip + ":6379" }

// --- tests -------------------------------------------------------------------

// TestReconcileMultiSetIsolation verifies that a healthy set is left alone while
// a mastered-less set fails over independently, and that no REPLICAOF ever
// crosses set boundaries.
func TestReconcileMultiSetIsolation(t *testing.T) {
	cs := fake.NewSimpleClientset(
		// "cache" set: healthy, cache-0 is master.
		testPod("cache-0", "10.0.0.1", "cache", kube.RoleMaster, nil),
		testPod("cache-1", "10.0.0.2", "cache", "", nil),
		// "sessions" set: no master; both replicas lost their link.
		testPod("sessions-0", "10.0.1.1", "sessions", "", nil),
		testPod("sessions-1", "10.0.1.2", "sessions", "", nil),
	)
	d := &fakeDialer{nodes: map[string]*fakeNode{
		addr("10.0.0.1"): {role: redis.RoleMaster, offset: 100},
		addr("10.0.0.2"): {role: redis.RoleReplica, offset: 90, linkStatus: "up", masterHost: "10.0.0.1"},
		addr("10.0.1.1"): {role: redis.RoleReplica, offset: 50, linkStatus: "down"},
		addr("10.0.1.2"): {role: redis.RoleReplica, offset: 60, linkStatus: "down"},
	}}

	c := New(testConfig(), kube.New(cs, testNS), d, discardLogger())
	if err := c.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// sessions-1 has the higher offset (60 > 50) => promoted.
	if !hasOp(d, op{addr: addr("10.0.1.2"), kind: opReplicaOfNoOne}) {
		t.Errorf("expected sessions-1 to be promoted (REPLICAOF NO ONE)")
	}
	// sessions-0 must be redirected to sessions-1, never to a cache node.
	if !hasOp(d, op{addr: addr("10.0.1.1"), kind: opReplicaOf, masterH: "10.0.1.2"}) {
		t.Errorf("expected sessions-0 to replicate from sessions-1; ops=%+v", d.ops)
	}
	// No write command may target a cache pod: the cache set was healthy.
	for _, o := range d.ops {
		if o.addr == addr("10.0.0.1") || o.addr == addr("10.0.0.2") {
			t.Errorf("cross-set or spurious op on cache pod: %+v", o)
		}
	}

	// Label outcomes.
	assertRole(t, cs, "cache-0", kube.RoleMaster)
	assertRole(t, cs, "cache-1", kube.RoleReplica)
	assertRole(t, cs, "sessions-1", kube.RoleMaster)
	assertRole(t, cs, "sessions-0", kube.RoleReplica)

	// First failover for the set => epoch 1.
	assertAnnotation(t, cs, "sessions-1", config.AnnotationFailoverEpoch, "1")
}

// TestFailoverEpochContinuity verifies the epoch is derived from existing pod
// annotations, so a fresh controller process does not regress the sequence.
func TestFailoverEpochContinuity(t *testing.T) {
	cs := fake.NewSimpleClientset(
		testPod("db-0", "10.0.2.1", "db", "", map[string]string{config.AnnotationFailoverEpoch: "5"}),
		testPod("db-1", "10.0.2.2", "db", "", nil),
	)
	d := &fakeDialer{nodes: map[string]*fakeNode{
		addr("10.0.2.1"): {role: redis.RoleReplica, offset: 200, linkStatus: "down"},
		addr("10.0.2.2"): {role: redis.RoleReplica, offset: 10, linkStatus: "down"},
	}}

	c := New(testConfig(), kube.New(cs, testNS), d, discardLogger())
	if err := c.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// db-0 has the higher offset (200) => promoted, epoch max(5)+1 = 6.
	if !hasOp(d, op{addr: addr("10.0.2.1"), kind: opReplicaOfNoOne}) {
		t.Fatalf("expected db-0 promoted; ops=%+v", d.ops)
	}
	assertAnnotation(t, cs, "db-0", config.AnnotationFailoverEpoch, "6")
}

// TestSelectFailoverCandidate covers the selection policy in isolation.
func TestSelectFailoverCandidate(t *testing.T) {
	mk := func(name string, ord int, off int64, link string) podInfo {
		return podInfo{
			pod:  kube.Pod{Name: name, Ordinal: ord},
			info: &redis.ReplicationInfo{Role: redis.RoleReplica, SlaveReplOffset: off, MasterLinkStatus: link},
		}
	}

	if got := selectFailoverCandidate(nil); got != nil {
		t.Errorf("empty input = %+v, want nil", got)
	}

	// Link-up replica is preferred even over a higher-offset link-down one.
	r := selectFailoverCandidate([]podInfo{
		mk("a", 0, 100, "down"),
		mk("b", 1, 50, "up"),
	})
	if r.pod.Name != "b" {
		t.Errorf("link preference: got %s, want b", r.pod.Name)
	}

	// Among link-up replicas, highest offset wins; ties break on lowest ordinal.
	r = selectFailoverCandidate([]podInfo{
		mk("a", 2, 80, "up"),
		mk("b", 1, 80, "up"),
		mk("c", 0, 70, "up"),
	})
	if r.pod.Name != "b" {
		t.Errorf("offset/ordinal: got %s, want b", r.pod.Name)
	}
}

// --- assertions --------------------------------------------------------------

func hasOp(d *fakeDialer, want op) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, o := range d.ops {
		if o == want {
			return true
		}
	}
	return false
}

func assertRole(t *testing.T, cs *fake.Clientset, pod, want string) {
	t.Helper()
	p, err := cs.CoreV1().Pods(testNS).Get(context.Background(), pod, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", pod, err)
	}
	if got := p.Labels[kube.LabelRole]; got != want {
		t.Errorf("%s role label = %q, want %q", pod, got, want)
	}
}

func assertAnnotation(t *testing.T, cs *fake.Clientset, pod, key, want string) {
	t.Helper()
	p, err := cs.CoreV1().Pods(testNS).Get(context.Background(), pod, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", pod, err)
	}
	if got := p.Annotations[key]; got != want {
		t.Errorf("%s annotation %s = %q, want %q", pod, key, got, want)
	}
}
