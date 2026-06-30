package server

import (
	"context"

	"github.com/tokenlive/tokenlive-gateway/pkg/log"
)

type JobServer struct {
	log     *log.Logger
}

func NewJobServer(
	log *log.Logger,
) *JobServer {
	return &JobServer{
		log:     log,
	}
}

func (j *JobServer) Start(ctx context.Context) error {
	// Tips: If you want job to start as a separate process, just refer to the task implementation and adjust the code accordingly.
	return nil
}
func (j *JobServer) Stop(ctx context.Context) error {
	return nil
}
