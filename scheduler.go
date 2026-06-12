package work

import (
	"math/rand"
	"time"

	"github.com/gomodule/redigo/redis"
)

type scheduler struct {
	namespace string
	pool      *redis.Pool
	periodics []*periodicJob

	stopChan chan struct{}
	doneChan chan struct{}
}

func newScheduler(namespace string, pool *redis.Pool, periodics []*periodicJob) *scheduler {
	return &scheduler{
		namespace: namespace,
		pool:      pool,
		periodics: periodics,
	}
}

func (s *scheduler) start() {
	s.stopChan = make(chan struct{})
	s.doneChan = make(chan struct{})

	go func() {
		defer close(s.doneChan)

		// Tick immediately on start
		s.tick()

		// Tick every 5 seconds or so
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-s.stopChan:
				return
			case <-ticker.C:
				// Sleep a random amount of time to prevent multiple schedulers from ticking at the exact same time
				time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
				s.tick()
			}
		}
	}()
}

func (s *scheduler) stop() {
	close(s.stopChan)
	<-s.doneChan
}

func (s *scheduler) tick() {
	conn := s.pool.Get()
	defer conn.Close()

	// Try to acquire the scheduler lock
	// The lock is held for 10 seconds
	lockKey := redisKeySchedLock(s.namespace)
	ok, err := redis.String(conn.Do("SET", lockKey, "1", "EX", "10", "NX"))
	if err != nil && err != redis.ErrNil {
		return
	}
	if ok != "OK" {
		return
	}

	now := time.Now()
	for _, p := range s.periodics {
		// Calculate the target execution tick precisely
		// Round to the period boundary
		targetTime := p.nextRun(now)
		if targetTime.IsZero() {
			continue
		}

		// Enqueue the job atomically with deduplication
		err := s.enqueuePeriodic(conn, p, targetTime)
		if err != nil {
			continue
		}
	}
}

func (s *scheduler) enqueuePeriodic(conn redis.Conn, p *periodicJob, targetTime time.Time) error {
	// Deduplication key: craft:work:sched_lock:<job_name>:<timestamp_seconds>
	timestamp := targetTime.Unix()
	dedupKey := redisKeySchedJobLock(s.namespace, p.name, timestamp)
	
	// Calculate TTL: 2 * period or a minimum of 5 minutes
	ttl := int64(p.period.Seconds() * 2)
	if ttl < 300 {
		ttl = 300
	}

	// Lua script to check-and-set the deduplication key and enqueue the job
	// KEYS[1] = dedupKey
	// KEYS[2] = queueKey
	// ARGV[1] = ttl
	// ARGV[2] = jobPayload
	// ARGV[3] = score (timestamp)
	script := redis.NewScript(2, `
		local exists = redis.call("EXISTS", KEYS[1])
		if exists == 1 then
			return 0
		end
		redis.call("SET", KEYS[1], "1", "EX", ARGV[1])
		redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
		return 1
	`)

	job := &Job{
		Name: p.name,
		ID:   makeJobID(),
		Args: nil,
	}
	payload, err := serializeJob(job)
	if err != nil {
		return err
	}

	queueKey := redisKeyScheduled(s.namespace)
	_, err = redis.Int(script.Do(conn, dedupKey, queueKey, ttl, payload, timestamp))
	return err
}
