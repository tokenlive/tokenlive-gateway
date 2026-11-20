package core

import "github.com/tokenlive/tokenlive-gateway/pkg/usage"

// Usage returns the normalized snapshot for this response attempt.
func (c *GatewayContext) Usage() usage.Tokens {
	return usage.Tokens{
		InputTokens: c.InputTokens, OutputTokens: c.OutputTokens,
		CachedTokens: c.CachedTokens, CacheCreationTokens: c.CacheCreationTokens,
	}
}

// MergeUsage applies normalized partial usage using the shared merge rules.
func (c *GatewayContext) MergeUsage(update usage.Tokens) {
	tokens := c.Usage()
	tokens.Merge(update)
	c.setUsage(tokens)
}

// ObserveUsage merges provider fields before normalizing them.
func (c *GatewayContext) ObserveUsage(update usage.Observation) {
	tokens := c.Usage()
	tokens.Observe(update)
	c.setUsage(tokens)
}

func (c *GatewayContext) setUsage(tokens usage.Tokens) {
	c.InputTokens = tokens.InputTokens
	c.OutputTokens = tokens.OutputTokens
	c.CachedTokens = tokens.CachedTokens
	c.CacheCreationTokens = tokens.CacheCreationTokens
}
