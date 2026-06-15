package core

import (
	"strings"
	"sync"
	"time"
)

// ActiveTurn 描述 WebSocket 会话中的单次生成交互（Turn）
type ActiveTurn struct {
	ResponseID     string
	TempID         string          // 临时 ID，在 response.create 发送但尚未拿到官方 response_id 时使用
	Tenant         string
	UserID         string
	Model          string
	PrePaidAmount  float64         // 预扣费金额/Quota
	StartTime      time.Time
	SentTextTokens int             // 预估已经下发给客户端的 Token 数量（用于异常断连时的粗估）
	IsSettled      bool            // 是否已结算，防止重复结算
	Gctx           *GatewayContext // 保存该 Turn 绑定的 GatewayContext 资源，用于结算和多退少补
}

// WSSessionTracker 管理单个 WebSocket 长连接内的多次生成交互
type WSSessionTracker struct {
	mu    sync.RWMutex
	turns map[string]*ActiveTurn
}

// NewWSSessionTracker 创建会话追踪器
func NewWSSessionTracker() *WSSessionTracker {
	return &WSSessionTracker{
		turns: make(map[string]*ActiveTurn),
	}
}

// AddTurn 添加交互记录
func (t *WSSessionTracker) AddTurn(key string, turn *ActiveTurn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.turns[key] = turn
}

// GetTurn 获取交互记录
func (t *WSSessionTracker) GetTurn(key string) *ActiveTurn {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.turns[key]
}

// AssociateLatestTempID 将最早的未关联临时 ID 绑定为官方 responseID（遵循 FIFO 先进先出时序）
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

// AssociateOfficialID 将临时 ID 与官方 responseID 关联，并更新 key
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

// RemoveTurn 移除并返回交互记录
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

// GetUnsettledTurns 找出所有未完成结算的交互，用于异常断连清理
func (t *WSSessionTracker) GetUnsettledTurns() []*ActiveTurn {
	t.mu.Lock()
	defer t.mu.Unlock()
	var unsettled []*ActiveTurn
	for _, turn := range t.turns {
		if !turn.IsSettled {
			unsettled = append(unsettled, turn)
		}
	}
	// 清空 turns 表
	t.turns = make(map[string]*ActiveTurn)
	return unsettled
}

