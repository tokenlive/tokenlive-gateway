package core

import (
	"strings"
	"sync"
	"time"
)

// ActiveTurn is one generation turn in a WebSocket session.
type ActiveTurn struct {
	ResponseID     string
	TempID         string          // Temp ID before official response_id is known
	Tenant         string
	UserID         string
	Model          string
	PrePaidAmount  float64         // Pre-paid amount/quota
	StartTime      time.Time
	SentTextTokens int             // Est. tokens already sent (for disconnect settlement)
	IsSettled      bool            // Settled; prevents double settlement
	Gctx           *GatewayContext // Bound GatewayContext for settlement
}

// WSSessionTracker tracks turns on one WebSocket connection.
type WSSessionTracker struct {
	mu    sync.RWMutex
	turns map[string]*ActiveTurn
}

// NewWSSessionTracker creates a session tracker.
func NewWSSessionTracker() *WSSessionTracker {
	return &WSSessionTracker{
		turns: make(map[string]*ActiveTurn),
	}
}

// AddTurn adds a turn.
func (t *WSSessionTracker) AddTurn(key string, turn *ActiveTurn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.turns[key] = turn
}

// GetTurn returns a turn by ID.
func (t *WSSessionTracker) GetTurn(key string) *ActiveTurn {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.turns[key]
}

// AssociateLatestTempID binds the oldest unbound temp ID to responseID (FIFO).
func (t *WSSessionTracker) AssociateLatestTempID(officialID string) *ActiveTurn {
	t.mu.Lock()
	defer t.mu.Unlock()

	var targetKey string
	var earliestTime time.Time

	for k, turn := range t.turns {
		if strings.HasPrefix(k, "temp_turn_") {
			if targetKey == "" || turn.StartTime.Before(earliestTime) {
				targetKey = k
				earliestTime = turn.StartTime
			}
		}
	}

	if targetKey != "" {
		turn := t.turns[targetKey]
		delete(t.turns, targetKey)
		turn.ResponseID = officialID
		t.turns[officialID] = turn
		return turn
	}
	return nil
}

// AssociateOfficialID links temp ID to official responseID.
func (t *WSSessionTracker) AssociateOfficialID(tempID, officialID string) *ActiveTurn {
	t.mu.Lock()
	defer t.mu.Unlock()
	turn, exists := t.turns[tempID]
	if exists {
		delete(t.turns, tempID)
		turn.ResponseID = officialID
		t.turns[officialID] = turn
		return turn
	}
	return nil
}

// RemoveTurn removes and returns a turn.
func (t *WSSessionTracker) RemoveTurn(key string) *ActiveTurn {
	t.mu.Lock()
	defer t.mu.Unlock()
	turn, exists := t.turns[key]
	if exists {
		delete(t.turns, key)
		return turn
	}
	return nil
}

// GetUnsettledTurns returns turns not yet settled (disconnect cleanup).
func (t *WSSessionTracker) GetUnsettledTurns() []*ActiveTurn {
	t.mu.Lock()
	defer t.mu.Unlock()
	var unsettled []*ActiveTurn
	for _, turn := range t.turns {
		if !turn.IsSettled {
			unsettled = append(unsettled, turn)
		}
	}
	// Clear turns map.
	t.turns = make(map[string]*ActiveTurn)
	return unsettled
}

