// Package kubernetes wraps the client-go calls the controller needs: listing
// Redis Pods, patching their role labels/annotations, and reading the
// EndpointSlices that back the redis-write Service.
package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgo "k8s.io/client-go/kubernetes"
)

// Label key and values used to route the redis-write Service to the master.
const (
	LabelRole   = "redis-current-role"
	RoleMaster  = "master"
	RoleReplica = "replica"
)

// Pod is the subset of Pod state the controller reasons about.
type Pod struct {
	Name        string
	Namespace   string
	IP          string
	Ordinal     int // trailing StatefulSet ordinal, or -1 if not parseable
	Phase       string
	Ready       bool
	Labels      map[string]string
	Annotations map[string]string
}

// RoleLabel returns the value of the controller's role label, or "".
func (p Pod) RoleLabel() string { return p.Labels[LabelRole] }

// HasMasterLabel reports whether the Pod is currently labeled as the master.
func (p Pod) HasMasterLabel() bool { return p.Labels[LabelRole] == RoleMaster }

// Client is a thin wrapper around a client-go clientset scoped to one namespace.
type Client struct {
	cs        clientgo.Interface
	namespace string
}

// New returns a Client backed by the given clientset and namespace.
func New(cs clientgo.Interface, namespace string) *Client {
	return &Client{cs: cs, namespace: namespace}
}

// Namespace returns the namespace this Client operates in.
func (c *Client) Namespace() string { return c.namespace }

// ListRedisPods lists Pods matching selector, sorted by ordinal (ascending,
// unknown ordinals last) for deterministic selection.
func (c *Client) ListRedisPods(ctx context.Context, selector string) ([]Pod, error) {
	list, err := c.cs.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list pods (selector=%q): %w", selector, err)
	}
	pods := make([]Pod, 0, len(list.Items))
	for i := range list.Items {
		pods = append(pods, toPod(&list.Items[i]))
	}
	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Ordinal != pods[j].Ordinal {
			return ordinalLess(pods[i].Ordinal, pods[j].Ordinal)
		}
		return pods[i].Name < pods[j].Name
	})
	return pods, nil
}

// SetRoleLabel sets the controller's role label on a Pod to value.
func (c *Client) SetRoleLabel(ctx context.Context, podName, value string) error {
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": map[string]interface{}{LabelRole: value},
		},
	}
	return c.patch(ctx, podName, patch)
}

// RemoveRoleLabel deletes the controller's role label from a Pod.
func (c *Client) RemoveRoleLabel(ctx context.Context, podName string) error {
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": map[string]interface{}{LabelRole: nil},
		},
	}
	return c.patch(ctx, podName, patch)
}

// SetAnnotations merges the given annotations onto a Pod.
func (c *Client) SetAnnotations(ctx context.Context, podName string, annotations map[string]string) error {
	m := make(map[string]interface{}, len(annotations))
	for k, v := range annotations {
		m[k] = v
	}
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{"annotations": m},
	}
	return c.patch(ctx, podName, patch)
}

func (c *Client) patch(ctx context.Context, podName string, patch map[string]interface{}) error {
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = c.cs.CoreV1().Pods(c.namespace).Patch(ctx, podName, types.MergePatchType, data, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("patch pod %s: %w", podName, err)
	}
	return nil
}

// EndpointSliceAddrs returns the ready endpoint addresses currently backing the
// named Service, used to validate that redis-write points only at the master.
func (c *Client) EndpointSliceAddrs(ctx context.Context, serviceName string) ([]string, error) {
	selector := "kubernetes.io/service-name=" + serviceName
	list, err := c.cs.DiscoveryV1().EndpointSlices(c.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list endpointslices for %s: %w", serviceName, err)
	}
	var addrs []string
	for i := range list.Items {
		for _, ep := range list.Items[i].Endpoints {
			if ep.Conditions.Ready != nil && !*ep.Conditions.Ready {
				continue
			}
			addrs = append(addrs, ep.Addresses...)
		}
	}
	return addrs, nil
}

// FindByIP returns the Pod with the given IP, if present.
func FindByIP(pods []Pod, ip string) (Pod, bool) {
	if ip == "" {
		return Pod{}, false
	}
	for _, p := range pods {
		if p.IP == ip {
			return p, true
		}
	}
	return Pod{}, false
}

func toPod(p *corev1.Pod) Pod {
	return Pod{
		Name:        p.Name,
		Namespace:   p.Namespace,
		IP:          p.Status.PodIP,
		Ordinal:     ordinalFromName(p.Name),
		Phase:       string(p.Status.Phase),
		Ready:       isReady(p),
		Labels:      copyMap(p.Labels),
		Annotations: copyMap(p.Annotations),
	}
}

func isReady(p *corev1.Pod) bool {
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// ordinalFromName parses the trailing integer of a StatefulSet Pod name
// (e.g. "redis-2" -> 2). Returns -1 when no ordinal is present.
func ordinalFromName(name string) int {
	i := strings.LastIndex(name, "-")
	if i < 0 || i == len(name)-1 {
		return -1
	}
	n, err := strconv.Atoi(name[i+1:])
	if err != nil {
		return -1
	}
	return n
}

// ordinalLess orders valid ordinals ascending and pushes unknown (-1) last.
func ordinalLess(a, b int) bool {
	switch {
	case a < 0:
		return false
	case b < 0:
		return true
	default:
		return a < b
	}
}

func copyMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
