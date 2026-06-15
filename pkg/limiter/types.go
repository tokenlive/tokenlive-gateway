package limiter

// HTTPError HTTP 错误，包含状态码和消息
type HTTPError struct {
	Code    int
	Message string
}

func (e *HTTPError) Error() string {
	return e.Message
}
