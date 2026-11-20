// Package usage decodes provider usage and merges cumulative observations for one
// response attempt. It has no dependency on transport or gateway lifecycle code.
package usage

// Tokens is a normalized usage snapshot. InputTokens includes cache reads and
// cache creation; both cache fields are subsets of input, never extra input.
type Tokens struct {
	InputTokens         int
	OutputTokens        int
	CachedTokens        int
	CacheCreationTokens int
}

// Merge applies a normalized partial snapshot. Upstreams sometimes send zero
// placeholders after valid usage, so non-positive values do not clear known data.
// Counts are cumulative snapshots: repeated observations are not added together.
func (t *Tokens) Merge(update Tokens) {
	if update.InputTokens > 0 {
		t.InputTokens = update.InputTokens
	}
	if update.OutputTokens > 0 {
		t.OutputTokens = update.OutputTokens
	}
	if update.CachedTokens > 0 {
		t.CachedTokens = update.CachedTokens
	}
	if update.CacheCreationTokens > 0 {
		t.CacheCreationTokens = update.CacheCreationTokens
	}
}

// Observation retains field presence and the provider's input accounting until
// it is merged with earlier frames. Its zero value carries no usage.
type Observation struct {
	input, output, cached, creation *int
	uncachedInput                   bool
}

// Decoder reads a JSON response or SSE data payload. Invalid JSON and payloads
// without usage return an empty observation and leave accumulated usage intact.
type Decoder func(string) Observation

// Observe merges a provider observation and returns its normalized partial
// update. Missing or non-positive fields stay zero in the returned update, so
// relaying it never replays unrelated history over a caller's newer snapshot.
// An Anthropic input/cache update also returns the recomputed total input.
func (t *Tokens) Observe(o Observation) Tokens {
	update := Tokens{
		InputTokens: positive(o.input), OutputTokens: positive(o.output),
		CachedTokens: positive(o.cached), CacheCreationTokens: positive(o.creation),
	}
	if o.uncachedInput && (update.InputTokens > 0 || update.CachedTokens > 0 || update.CacheCreationTokens > 0) {
		uncached := max(0, t.InputTokens-t.CachedTokens-t.CacheCreationTokens)
		if update.InputTokens > 0 {
			uncached = update.InputTokens
		}
		next := *t
		next.Merge(update)
		update.InputTokens = uncached + next.CachedTokens + next.CacheCreationTokens
	}
	t.Merge(update)
	return update
}

func positive(value *int) int {
	if value == nil {
		return 0
	}
	return max(0, *value)
}
