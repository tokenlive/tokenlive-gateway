package lbs

import (
	"fmt"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func hashTestEndpoints(n int) []*core.Endpoint {
	eps := make([]*core.Endpoint, 0, n)
	for i := 0; i < n; i++ {
		eps = append(eps, &core.Endpoint{ID: "ep" + string(rune('a'+i)), Weight: 1})
	}
	return eps
}

func TestSessionHash_EmptyEndpoints(t *testing.T) {
	lb := NewSessionHashLoadBalancer(nil)
	if inv := lb.Select(&core.GatewayContext{SessionID: "s1"}, nil); inv != nil {
		t.Errorf("expected nil invoker for empty endpoints")
	}
}

func TestSessionHash_EmptySessionFallsBack(t *testing.T) {
	fallback := NewRoundRobin()
	lb := NewSessionHashLoadBalancer(fallback)
	eps := hashTestEndpoints(3)
	inv := lb.Select(&core.GatewayContext{}, eps)
	if inv == nil {
		t.Fatalf("expected fallback invoker")
	}
}

func TestSessionHash_SameSessionSameEndpoint(t *testing.T) {
	lb := NewSessionHashLoadBalancer(nil)
	eps := hashTestEndpoints(3)
	gctx := &core.GatewayContext{SessionID: "session-abc"}
	first := lb.Select(gctx, eps)
	if first == nil {
		t.Fatalf("expected invoker")
	}
	want := first.Endpoint().ID
	// Repeated selection and shuffled input order must agree.
	for i := 0; i < 20; i++ {
		shuffled := shuffleEndpoints(eps)
		inv := lb.Select(gctx, shuffled)
		if got := inv.Endpoint().ID; got != want {
			t.Fatalf("session remapped: want %s got %s", want, got)
		}
	}
}

func TestSessionHash_DifferentSessionsDistributed(t *testing.T) {
	lb := NewSessionHashLoadBalancer(nil)
	eps := hashTestEndpoints(3)
	seen := map[string]bool{}
	for i := 0; i < 30; i++ {
		gctx := &core.GatewayContext{SessionID: fmt.Sprintf("session-%02d", i)}
		inv := lb.Select(gctx, eps)
		if inv == nil {
			t.Fatalf("expected invoker")
		}
		seen[inv.Endpoint().ID] = true
	}
	if len(seen) < 2 {
		t.Errorf("expected sessions spread over multiple endpoints, got %v", seen)
	}
}

func TestSessionHash_EndpointRemovalKeepsOthersStable(t *testing.T) {
	lb := NewSessionHashLoadBalancer(nil)
	eps := hashTestEndpoints(3)
	pick := func(sid string, list []*core.Endpoint) string {
		inv := lb.Select(&core.GatewayContext{SessionID: sid}, list)
		if inv == nil {
			t.Fatalf("expected invoker")
		}
		return inv.Endpoint().ID
	}
	// Sessions not bound to the removed endpoint must keep their binding.
	kept := 0
	for i := 0; i < 30; i++ {
		sid := fmt.Sprintf("session-%02d", i)
		before := pick(sid, eps)
		if before == "epc" {
			continue
		}
		kept++
		reduced := []*core.Endpoint{eps[0], eps[1]}
		if after := pick(sid, reduced); after != before {
			t.Fatalf("session %s remapped after unrelated endpoint removal: %s -> %s", sid, before, after)
		}
	}
	if kept == 0 {
		t.Skip("no sessions bound to surviving endpoints in this run")
	}
}

func TestSessionHash_WeightAffectsDistribution(t *testing.T) {
	lb := NewSessionHashLoadBalancer(nil)
	heavy := &core.Endpoint{ID: "ep-heavy", Weight: 50}
	light := &core.Endpoint{ID: "ep-light", Weight: 1}
	eps := []*core.Endpoint{heavy, light}
	const sessions = 2000
	counts := map[string]int{}
	for i := 0; i < sessions; i++ {
		gctx := &core.GatewayContext{SessionID: fmt.Sprintf("session-%04d", i)}
		inv := lb.Select(gctx, eps)
		if inv == nil {
			t.Fatalf("expected invoker")
		}
		counts[inv.Endpoint().ID]++
	}
	// HRW replica weighting does not split sessions linearly by weight;
	// assert the heavy endpoint gets a clear majority instead.
	if counts["ep-heavy"] <= sessions/2 {
		t.Errorf("expected heavy endpoint to receive the majority of sessions, got %v", counts)
	}
}

func shuffleEndpoints(eps []*core.Endpoint) []*core.Endpoint {
	out := make([]*core.Endpoint, len(eps))
	copy(out, eps)
	// Deterministic rotate to vary input order.
	rot := out[0]
	out = out[1:]
	out = append(out, rot)
	return out
}

// --- 动态增删端点下的稳定性 ---

func namedEndpoints(n int) []*core.Endpoint {
	eps := make([]*core.Endpoint, n)
	for i := range eps {
		eps[i] = &core.Endpoint{ID: fmt.Sprintf("ep-%02d", i), Weight: 1}
	}
	return eps
}

func removeEndpointByID(eps []*core.Endpoint, id string) []*core.Endpoint {
	out := make([]*core.Endpoint, 0, len(eps))
	for _, ep := range eps {
		if ep.ID != id {
			out = append(out, ep)
		}
	}
	return out
}

// TestSessionHash_ScaleDownKeepsBindings 模拟端点逐个下线（缩容/熔断剔除）：
// 只要会话绑定的端点仍在候选集合中，会话必须保持原绑定；
// 仅当自身绑定端点被移除时才允许重映射。
func TestSessionHash_ScaleDownKeepsBindings(t *testing.T) {
	lb := NewSessionHashLoadBalancer(nil)
	eps := namedEndpoints(6)
	const sessions = 1000

	pick := func(sid string, list []*core.Endpoint) string {
		inv := lb.Select(&core.GatewayContext{SessionID: sid}, list)
		if inv == nil {
			t.Fatalf("expected invoker for session %s", sid)
		}
		return inv.Endpoint().ID
	}
	sessionID := func(i int) string { return fmt.Sprintf("sess-%04d", i) }

	binding := make(map[string]string, sessions)
	for i := 0; i < sessions; i++ {
		sid := sessionID(i)
		binding[sid] = pick(sid, eps)
	}

	current := append([]*core.Endpoint(nil), eps...)
	for _, victim := range []string{"ep-04", "ep-01", "ep-05", "ep-02"} {
		current = removeEndpointByID(current, victim)
		for i := 0; i < sessions; i++ {
			sid := sessionID(i)
			got := pick(sid, current)
			if binding[sid] == victim {
				// 自身端点被移除，允许重映射并记录新绑定。
				binding[sid] = got
			} else if got != binding[sid] {
				t.Fatalf("session %s moved from %s to %s while its endpoint survived (removed %s)",
					sid, binding[sid], got, victim)
			}
		}
	}

	// 全部缩容结束后，绑定只能落在幸存端点上。
	for i := 0; i < sessions; i++ {
		sid := sessionID(i)
		if binding[sid] != "ep-00" && binding[sid] != "ep-03" {
			t.Fatalf("session %s bound to non-surviving endpoint %s", sid, binding[sid])
		}
	}
}

// TestSessionHash_ScaleUpOnlyNewEndpointSteals 模拟新增端点（扩容）：
// 允许被"抢走"的会话只能迁移到新端点，且迁移比例必须接近理论下界 1/(n+1)。
// 这是无状态哈希的理论极限：新端点必须承接流量，否则扩容毫无意义。
func TestSessionHash_ScaleUpOnlyNewEndpointSteals(t *testing.T) {
	lb := NewSessionHashLoadBalancer(nil)
	eps := namedEndpoints(4)
	const sessions = 2000

	pick := func(sid string, list []*core.Endpoint) string {
		inv := lb.Select(&core.GatewayContext{SessionID: sid}, list)
		if inv == nil {
			t.Fatalf("expected invoker for session %s", sid)
		}
		return inv.Endpoint().ID
	}
	sessionID := func(i int) string { return fmt.Sprintf("sess-%04d", i) }

	before := make(map[string]string, sessions)
	for i := 0; i < sessions; i++ {
		sid := sessionID(i)
		before[sid] = pick(sid, eps)
	}

	expanded := append(append([]*core.Endpoint(nil), eps...), &core.Endpoint{ID: "ep-new", Weight: 1})
	moved := 0
	for i := 0; i < sessions; i++ {
		sid := sessionID(i)
		got := pick(sid, expanded)
		if got == before[sid] {
			continue
		}
		moved++
		if got != "ep-new" {
			t.Fatalf("session %s moved from %s to %s: only the new endpoint may steal sessions",
				sid, before[sid], got)
		}
	}
	if moved == 0 {
		t.Fatal("new endpoint stole no sessions; it would receive no traffic")
	}
	// 理论迁移比例为 1/(4+1)=20%；放宽到 1/3 以内判定异常。
	if moved > sessions/3 {
		t.Errorf("scale-up disruption too high: %d/%d sessions moved", moved, sessions)
	}
	t.Logf("scale-up 4->5 endpoints: %d/%d sessions moved to the new endpoint (%.1f%%)",
		moved, sessions, float64(moved)*100/float64(sessions))
}

// TestSessionHash_RemovedEndpointReturnsRestoresBinding 模拟熔断恢复：
// 端点被临时剔除后会话重映射；端点恢复后，原有会话必须回到原端点。
// 这是 HRW 每对 (session, endpoint) 分数独立于集合的性质的直接推论。
func TestSessionHash_RemovedEndpointReturnsRestoresBinding(t *testing.T) {
	lb := NewSessionHashLoadBalancer(nil)
	eps := namedEndpoints(5)
	const sessions = 1000

	pick := func(sid string, list []*core.Endpoint) string {
		inv := lb.Select(&core.GatewayContext{SessionID: sid}, list)
		if inv == nil {
			t.Fatalf("expected invoker for session %s", sid)
		}
		return inv.Endpoint().ID
	}
	sessionID := func(i int) string { return fmt.Sprintf("sess-%04d", i) }

	original := make(map[string]string, sessions)
	for i := 0; i < sessions; i++ {
		sid := sessionID(i)
		original[sid] = pick(sid, eps)
	}

	victim := "ep-03"
	reduced := removeEndpointByID(eps, victim)
	affected := 0
	for i := 0; i < sessions; i++ {
		sid := sessionID(i)
		if original[sid] != victim {
			continue
		}
		affected++
		got := pick(sid, reduced)
		if got == victim {
			t.Fatalf("session %s still bound to removed endpoint %s", sid, victim)
		}
		// 恢复 victim 后必须回到原绑定。
		if back := pick(sid, eps); back != victim {
			t.Fatalf("session %s did not return to restored endpoint %s (got %s)", sid, victim, back)
		}
	}
	if affected == 0 {
		t.Skip("no sessions bound to victim endpoint in this run")
	}
	t.Logf("circuit-break drill: %d/%d sessions affected by removal all returned on restore", affected, sessions)
}
