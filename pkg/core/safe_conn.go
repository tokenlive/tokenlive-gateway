package core

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// SafeWebSocketConn is a concurrency-safe WebSocket conn wrapper.
type SafeWebSocketConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

// NewSafeWebSocketConn wraps conn with write mutexes.
func NewSafeWebSocketConn(conn *websocket.Conn) *SafeWebSocketConn {
	return &SafeWebSocketConn{conn: conn}
}

// WriteMessage writes a data message (Text/Binary).
func (s *SafeWebSocketConn) WriteMessage(messageType int, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteMessage(messageType, data)
}

// WriteJSON writes a JSON message.
func (s *SafeWebSocketConn) WriteJSON(v interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteJSON(v)
}

// WriteControl writes a control frame (Ping/Pong/Close).
func (s *SafeWebSocketConn) WriteControl(messageType int, data []byte, deadline time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteControl(messageType, data, deadline)
}

// ReadMessage reads a message (no lock; single reader expected).
func (s *SafeWebSocketConn) ReadMessage() (messageType int, p []byte, err error) {
	return s.conn.ReadMessage()
}

// Close closes the underlying connection.
func (s *SafeWebSocketConn) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.Close()
}

// UnderlyingConn returns the raw *websocket.Conn.
func (s *SafeWebSocketConn) UnderlyingConn() *websocket.Conn {
	return s.conn
}
