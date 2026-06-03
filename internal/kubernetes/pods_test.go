package kubernetes

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const testNS = "redis"

func newPod(name, ip string, ready bool, labels map[string]string) *corev1.Pod {
	cond := corev1.ConditionFalse
	if ready {
		cond = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: labels},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			PodIP:      ip,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: cond}},
		},
	}
}

func TestListRedisPodsSortedAndParsed(t *testing.T) {
	cs := fake.NewSimpleClientset(
		// Inserted out of order to exercise ordinal sorting.
		newPod("redis-10", "10.0.0.20", false, map[string]string{"app": "redis"}),
		newPod("redis-2", "10.0.0.12", true, map[string]string{"app": "redis", LabelRole: RoleMaster}),
		newPod("redis-0", "10.0.0.10", true, map[string]string{"app": "redis"}),
	)
	c := New(cs, testNS)

	pods, err := c.ListRedisPods(context.Background(), "app=redis")
	if err != nil {
		t.Fatal(err)
	}
	if len(pods) != 3 {
		t.Fatalf("got %d pods, want 3", len(pods))
	}
	wantOrder := []string{"redis-0", "redis-2", "redis-10"}
	for i, name := range wantOrder {
		if pods[i].Name != name {
			t.Fatalf("pods[%d] = %s, want %s (order %v)", i, pods[i].Name, name, wantOrder)
		}
	}
	if pods[0].Ordinal != 0 || pods[2].Ordinal != 10 {
		t.Fatalf("ordinals = %d, %d", pods[0].Ordinal, pods[2].Ordinal)
	}
	if pods[1].IP != "10.0.0.12" || !pods[1].HasMasterLabel() {
		t.Fatalf("redis-2 = %+v", pods[1])
	}
	if !pods[0].Ready || pods[2].Ready {
		t.Fatalf("readiness wrong: %v / %v", pods[0].Ready, pods[2].Ready)
	}
}

func TestOrdinalFromName(t *testing.T) {
	cases := map[string]int{
		"redis-0":       0,
		"redis-12":      12,
		"redis":         -1,
		"redis-":        -1,
		"redis-abc":     -1,
		"my-redis-3":    3,
		"redis-0-extra": -1,
	}
	for name, want := range cases {
		if got := ordinalFromName(name); got != want {
			t.Errorf("ordinalFromName(%q) = %d, want %d", name, got, want)
		}
	}
}

func TestFindByIP(t *testing.T) {
	pods := []Pod{{Name: "a", IP: "1.1.1.1"}, {Name: "b", IP: "2.2.2.2"}}
	if p, ok := FindByIP(pods, "2.2.2.2"); !ok || p.Name != "b" {
		t.Fatalf("FindByIP = %+v, %v", p, ok)
	}
	if _, ok := FindByIP(pods, "9.9.9.9"); ok {
		t.Fatal("expected not found")
	}
	if _, ok := FindByIP(pods, ""); ok {
		t.Fatal("empty IP must not match")
	}
}

func TestSetAndRemoveRoleLabel(t *testing.T) {
	cs := fake.NewSimpleClientset(newPod("redis-0", "10.0.0.10", true, map[string]string{"app": "redis"}))
	c := New(cs, testNS)
	ctx := context.Background()

	if err := c.SetRoleLabel(ctx, "redis-0", RoleMaster); err != nil {
		t.Fatal(err)
	}
	p, _ := cs.CoreV1().Pods(testNS).Get(ctx, "redis-0", metav1.GetOptions{})
	if p.Labels[LabelRole] != RoleMaster {
		t.Fatalf("label = %q, want master", p.Labels[LabelRole])
	}

	if err := c.RemoveRoleLabel(ctx, "redis-0"); err != nil {
		t.Fatal(err)
	}
	p, _ = cs.CoreV1().Pods(testNS).Get(ctx, "redis-0", metav1.GetOptions{})
	if _, ok := p.Labels[LabelRole]; ok {
		t.Fatalf("role label not removed: %v", p.Labels)
	}
	if p.Labels["app"] != "redis" {
		t.Fatalf("unrelated label was modified: %v", p.Labels)
	}
}

func TestSetAnnotations(t *testing.T) {
	cs := fake.NewSimpleClientset(newPod("redis-0", "10.0.0.10", true, map[string]string{"app": "redis"}))
	c := New(cs, testNS)
	ctx := context.Background()
	if err := c.SetAnnotations(ctx, "redis-0", map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	p, _ := cs.CoreV1().Pods(testNS).Get(ctx, "redis-0", metav1.GetOptions{})
	if p.Annotations["k"] != "v" {
		t.Fatalf("annotation = %q", p.Annotations["k"])
	}
}

func TestListRedisPodsAPIFailure(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("api server unavailable")
	})
	c := New(cs, testNS)
	if _, err := c.ListRedisPods(context.Background(), "app=redis"); err == nil {
		t.Fatal("expected error when API fails")
	}
}
