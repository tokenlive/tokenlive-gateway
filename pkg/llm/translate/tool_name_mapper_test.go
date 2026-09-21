package translate

import "testing"

func TestToolNameMapper_SanitizeNamespaceDot(t *testing.T) {
	m := NewToolNameMapper()
	got := m.SanitizeAndRegister("collaboration", "spawn_agent")
	if got != "collaboration_spawn_agent" {
		t.Fatalf("sanitized = %q, want collaboration_spawn_agent", got)
	}
	ns, name := m.Restore(got)
	if ns != "collaboration" || name != "spawn_agent" {
		t.Fatalf("restore = (%q,%q), want (collaboration,spawn_agent)", ns, name)
	}
}

func TestToolNameMapper_NoNamespacePassthrough(t *testing.T) {
	m := NewToolNameMapper()
	got := m.SanitizeAndRegister("", "apply_patch")
	if got != "apply_patch" {
		t.Fatalf("sanitized = %q, want apply_patch", got)
	}
	ns, name := m.Restore(got)
	if ns != "" || name != "apply_patch" {
		t.Fatalf("restore = (%q,%q), want (\"\",apply_patch)", ns, name)
	}
}

func TestToolNameMapper_IllegalCharsReplaced(t *testing.T) {
	m := NewToolNameMapper()
	// colons, dots, slashes are all illegal under ^[a-zA-Z0-9_-]+$
	got := m.SanitizeAndRegister("browser-use", "control.browser")
	if got != "browser-use_control_browser" {
		t.Fatalf("sanitized = %q, want browser-use_control_browser", got)
	}
	// must be pattern-safe
	for _, r := range got {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-'
		if !ok {
			t.Fatalf("sanitized name %q contains illegal char %q", got, r)
		}
	}
	ns, name := m.Restore(got)
	if ns != "browser-use" || name != "control.browser" {
		t.Fatalf("restore = (%q,%q), want (browser-use,control.browser)", ns, name)
	}
}

func TestToolNameMapper_CollisionDedup(t *testing.T) {
	m := NewToolNameMapper()
	a := m.SanitizeAndRegister("browser", "navigate") // -> browser_navigate
	b := m.SanitizeAndRegister("", "browser_navigate") // collides -> browser_navigate_2
	if a == b {
		t.Fatalf("collision not deduped: both = %q", a)
	}
	if a != "browser_navigate" {
		t.Fatalf("a = %q, want browser_navigate", a)
	}
	if b != "browser_navigate_2" {
		t.Fatalf("b = %q, want browser_navigate_2", b)
	}
	nsA, nameA := m.Restore(a)
	if nsA != "browser" || nameA != "navigate" {
		t.Fatalf("restore a = (%q,%q)", nsA, nameA)
	}
	nsB, nameB := m.Restore(b)
	if nsB != "" || nameB != "browser_navigate" {
		t.Fatalf("restore b = (%q,%q), want (\"\",browser_navigate)", nsB, nameB)
	}
}

func TestToolNameMapper_Idempotent(t *testing.T) {
	m := NewToolNameMapper()
	first := m.SanitizeAndRegister("collaboration", "spawn_agent")
	second := m.SanitizeAndRegister("collaboration", "spawn_agent")
	if first != second {
		t.Fatalf("not idempotent: %q != %q", first, second)
	}
}

func TestToolNameMapper_RestoreUnknownFallsBackToDotSplit(t *testing.T) {
	m := NewToolNameMapper()
	// a name never registered: fallback to dot-split heuristic
	ns, name := m.Restore("some.unknown")
	if ns != "some" || name != "unknown" {
		t.Fatalf("fallback restore = (%q,%q), want (some,unknown)", ns, name)
	}
}
