package server

import (
	"context"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/log"

	"github.com/go-co-op/gocron"
	"go.uber.org/zap"
)

type TaskServer struct {
	log       *log.Logger
	scheduler *gocron.Scheduler
}

func NewTaskServer(
	log *log.Logger,
) *TaskServer {
	return &TaskServer{
		log:      log,
	}
}
func (t *TaskServer) Start(ctx context.Context) error {
	gocron.SetPanicHandler(func(jobName string, recoverData interface{}) {
		t.log.Error("TaskServer Panic", zap.String("job", jobName), zap.Any("recover", recoverData))
	})

	// eg: crontab task
	t.scheduler = gocron.NewScheduler(time.UTC)
	// if you are in China, you will need to change the time zone as follows
	// t.scheduler = gocron.NewScheduler(time.FixedZone("PRC", 8*60*60))

	t.scheduler.StartBlocking()
	return nil
}
func (t *TaskServer) Stop(ctx context.Context) error {
	t.scheduler.Stop()
	t.log.Info("TaskServer stop...")
	return nil
}
