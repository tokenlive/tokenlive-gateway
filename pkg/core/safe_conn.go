package core

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// SafeWebSocketConn 并发安全的 WebSocket 连接封装，防范 Read/Write 的并发冲突问题
type SafeWebSocketConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

// NewSafeWebSocketConn 创建并发安全的包装器
func NewSafeWebSocketConn(conn *websocket.Conn) *SafeWebSocketConn {
	return &SafeWebSocketConn{conn: conn}
}

// WriteMessage 写入普通数据消息 (Text/Binary)
func (s *SafeWebSocketConn) WriteMessage(messageType int, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteMessage(messageType, data)
}

// WriteJSON 写入 JSON 数据
func (s *SafeWebSocketConn) WriteJSON(v interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteJSON(v)
}

// WriteControl 写入控制数据消息 (Ping/Pong/Close)
func (s *SafeWebSocketConn) WriteControl(messageType int, data []byte, deadline time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteControl(messageType, data, deadline)
}

// ReadMessage 读取消息（Read 不需要加锁，因为读循环通常是由单一协程顺序执行的）
func (s *SafeWebSocketConn) ReadMessage() (messageType int, p []byte, err error) {
	return s.conn.ReadMessage()
}

// Close 关闭底层连接
func (s *SafeWebSocketConn) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.Close()
}

// UnderlyingConn 获取底层原生的 *websocket.Conn
func (s *SafeWebSocketConn) UnderlyingConn() *websocket.Conn {
	return s.conn
}
