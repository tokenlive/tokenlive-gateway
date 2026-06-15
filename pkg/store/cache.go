package store

import (
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

type ExpirableCache[K comparable, V any] struct {
	validCache   *expirable.LRU[K, V]
	invalidCache *expirable.LRU[K, string]
}

func NewExpirableCache[K comparable, V any](validCap int, validTTL time.Duration, invalidCap int, invalidTTL time.Duration) *ExpirableCache[K, V] {
	return &ExpirableCache[K, V]{
		validCache: expirable.NewLRU[K, V](
			validCap,
			func(key K, value V) {},
			validTTL,
		),
		invalidCache: expirable.NewLRU[K, string](
			invalidCap,
			func(key K, value string) {},
			invalidTTL,
		),
	}
}

// Get queries valid and invalid caches. Returns (value, errorMsg, exists).
func (c *ExpirableCache[K, V]) Get(key K) (V, string, bool) {
	if errMsg, ok := c.invalidCache.Get(key); ok {
		var zero V
		return zero, errMsg, true
	}
	if val, ok := c.validCache.Get(key); ok {
		return val, "", true
	}
	var zero V
	return zero, "", false
}

func (c *ExpirableCache[K, V]) AddValid(key K, val V) {
	c.validCache.Add(key, val)
}

func (c *ExpirableCache[K, V]) AddInvalid(key K, errMsg string) {
	c.invalidCache.Add(key, errMsg)
}

func (c *ExpirableCache[K, V]) Remove(key K) {
	c.validCache.Remove(key)
	c.invalidCache.Remove(key)
}
