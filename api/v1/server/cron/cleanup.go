package cron

import (
	"context"
	"time"
)

func (c *CronScheduler) runUserSessionsCleanup(ctx context.Context) func() {
	return func() {
		ctx, cancel := context.WithTimeout(ctx, time.Second*20)
		defer cancel()

		deletedCount, err := c.repoV1.UserSession().CleanupUserSessions(ctx)
		if err != nil {
			if deletedCount > 0 {
				c.l.Err(err).Ctx(ctx).Int("deleted_count", deletedCount).Msg("user sessions cleanup cron job failed partially")
			} else {
				c.l.Err(err).Ctx(ctx).Msg("user sessions cleanup cron job failed")
			}
			return
		}

		c.l.Info().Ctx(ctx).Int("deleted_count", deletedCount).Msg("user sessions cleanup cron job completed")
	}
}
