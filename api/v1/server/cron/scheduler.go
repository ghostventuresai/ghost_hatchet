package cron

import (
	"context"
	"fmt"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog"

	"github.com/hatchet-dev/hatchet/pkg/config/server"
	"github.com/hatchet-dev/hatchet/pkg/repository"
)

type CronScheduler struct {
	s      gocron.Scheduler
	l      *zerolog.Logger
	repoV1 repository.Repository
}

func NewScheduler(c *server.ServerConfig) (*CronScheduler, error) {
	s, err := gocron.NewScheduler(gocron.WithLocation(time.UTC))
	if err != nil {
		return nil, fmt.Errorf("could not create cron scheduler: %w", err)
	}

	return &CronScheduler{
		s:      s,
		l:      c.Logger,
		repoV1: c.V1,
	}, nil
}

func (c *CronScheduler) Start() (func() error, error) {
	ctx, cancel := context.WithCancel(context.Background())

	c.l.Debug().Ctx(ctx).Msgf("starting cron scheduler")

	_, err := c.s.NewJob(
		gocron.DurationJob(time.Hour*1),
		gocron.NewTask(c.runUserSessionsCleanup(ctx)),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("could not register job: %w", err)
	}

	c.s.Start()

	cleanup := func() error {
		c.l.Debug().Ctx(ctx).Msg("stopping cron scheduler")
		cancel()

		if err := c.s.Shutdown(); err != nil {
			return fmt.Errorf("could not shutdown cron scheduler: %w", err)
		}

		return nil
	}

	return cleanup, nil
}
