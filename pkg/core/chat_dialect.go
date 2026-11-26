package core

import "net/http"

// ChatDialectError reports a local request rejection before contacting an upstream.
type ChatDialectError string

func (e ChatDialectError) Error() string   { return string(e) }
func (e ChatDialectError) Code() int       { return http.StatusBadRequest }
func (e ChatDialectError) Retryable() bool { return false }
