//go:build go1.11
// +build go1.11

package ocsql

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

// RecordStats records database statistics for provided sql.DB at the provided
// interval.
func RecordStats(db *sql.DB, interval time.Duration, poolName string) (fnStop func()) {
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

	var tags []tag.Mutator
	if poolName != "" {
		tags = append(tags, tag.Insert(PoolName, poolName))
	}

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

				percentActive := 100 * float64(dbStats.InUse) / float64(dbStats.MaxOpenConnections)

				err := stats.RecordWithTags(ctx, tags,
					MeasureOpenConnections.M(int64(dbStats.OpenConnections)),
					MeasureIdleConnections.M(int64(dbStats.Idle)),
					MeasureActiveConnections.M(int64(dbStats.InUse)),
					MeasurePercentActiveConnections.M(int64(percentActive)),
					MeasureWaitCount.M(dbStats.WaitCount),
					MeasureWaitDuration.M(float64(dbStats.WaitDuration.Nanoseconds())/1e6),
					MeasureAvgWaitPerConnection.M(avgWaitPerConnection),
					MeasureIdleClosed.M(dbStats.MaxIdleClosed),
					MeasureLifetimeClosed.M(dbStats.MaxLifetimeClosed),
				)

				if err != nil {
					log.Printf("ocsql.RecordStats: failed to record stats: %v\n", err)
				}

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
