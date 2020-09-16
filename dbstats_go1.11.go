// +build go1.11

package ocsql

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"go.opencensus.io/stats"
)

// RecordStats records database statistics for provided sql.DB at the provided
// interval.
func RecordStats(db *sql.DB, interval time.Duration) (fnStop func()) {
	var (
		closeOnce sync.Once
		ctx       = context.Background()
		ticker    = time.NewTicker(interval)
		done      = make(chan struct{})
	)

	var (
		lastWaitCount    int64
		lastWaitDuration time.Duration
	)

	go func() {
		for {
			select {
			case <-ticker.C:
				dbStats := db.Stats()

				lastWindowWaitCount := dbStats.WaitCount - lastWaitCount
				lastWindowWaitDuration := dbStats.WaitDuration - lastWaitDuration

				var avgWaitPerConnection float64
				if lastWindowWaitCount > 0 {
					avgWaitPerConnection = float64(lastWindowWaitDuration.Milliseconds()) / float64(lastWindowWaitCount)
				}

				stats.Record(ctx,
					MeasureOpenConnections.M(int64(dbStats.OpenConnections)),
					MeasureIdleConnections.M(int64(dbStats.Idle)),
					MeasureActiveConnections.M(int64(dbStats.InUse)),
					MeasureWaitCount.M(dbStats.WaitCount),
					MeasureWaitDuration.M(float64(dbStats.WaitDuration.Nanoseconds())/1e6),
					MeasureAvgWaitPerConnection.M(avgWaitPerConnection),
					MeasureIdleClosed.M(dbStats.MaxIdleClosed),
					MeasureLifetimeClosed.M(dbStats.MaxLifetimeClosed),
				)

				lastWaitCount = dbStats.WaitCount
				lastWaitDuration = dbStats.WaitDuration
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()

	return func() {
		closeOnce.Do(func() {
			close(done)
		})
	}
}
