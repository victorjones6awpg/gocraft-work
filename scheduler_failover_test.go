package work

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPeriodicEnqueuerConcurrentFailover(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	var pjs []*periodicJob
	pjs = appendPeriodicJob(pjs, "0/29 * * * * *", "foo") // Every 29 seconds

	setNowEpochSecondsMock(1468359453)
	defer resetNowEpochSecondsMock()

	pe1 := newPeriodicEnqueuer(ns, pool, pjs)
	pe2 := newPeriodicEnqueuer(ns, pool, pjs)

	// Node 1 enqueues
	err := pe1.enqueue()
	assert.NoError(t, err)

	c := NewClient(ns, pool)
	_, count, err := c.ScheduledJobs(1)
	assert.NoError(t, err)
	assert.True(t, count > 0, "Node 1 should have scheduled jobs")

	// Simulate requeuer or worker popping all jobs from the scheduled queue
	conn := pool.Get()
	defer conn.Close()
	_, err = conn.Do("DEL", redisKeyScheduled(ns))
	assert.NoError(t, err)

	// Node 2 tries to enqueue for the exact same tick window
	err = pe2.enqueue()
	assert.NoError(t, err)

	// Because Node 1 acquired the Lua lock, Node 2 should skip ZADD
	_, count2, err := c.ScheduledJobs(1)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, count2, "Node 2 should not schedule duplicate jobs because of the lock")
}
