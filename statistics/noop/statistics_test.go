package noop

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoOpStatistics_GetWorkerStats(t *testing.T) {
	stats := NewStatistics()
	ctx := context.Background()

	workerStats, err := stats.GetWorkerStats(ctx, "test-worker")
	require.NoError(t, err)

	assert.Equal(t, "test-worker", workerStats.ID)
	assert.Equal(t, int64(0), workerStats.Processed)
	assert.Equal(t, int64(0), workerStats.Failed)
	assert.Equal(t, int64(0), workerStats.InProgress)
	assert.True(t, workerStats.StartTime.IsZero())
	assert.True(t, workerStats.LastJob.IsZero())
}

func TestNoOpStatistics_GetQueueStats(t *testing.T) {
	stats := NewStatistics()
	ctx := context.Background()

	queueStats, err := stats.GetQueueStats(ctx, "test-queue")
	require.NoError(t, err)

	assert.Equal(t, "test-queue", queueStats.Name)
	assert.Equal(t, int64(0), queueStats.Length)
	assert.Equal(t, int64(0), queueStats.Processed)
	assert.Equal(t, int64(0), queueStats.Failed)
	assert.Equal(t, int64(0), queueStats.Workers)
}

func TestNoOpStatistics_GetGlobalStats(t *testing.T) {
	stats := NewStatistics()
	ctx := context.Background()

	globalStats, err := stats.GetGlobalStats(ctx)
	require.NoError(t, err)

	assert.Equal(t, int64(0), globalStats.TotalProcessed)
	assert.Equal(t, int64(0), globalStats.TotalFailed)
	assert.Equal(t, int64(0), globalStats.ActiveWorkers)
	assert.NotNil(t, globalStats.QueueStats)
	assert.Len(t, globalStats.QueueStats, 0)
}
