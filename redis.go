package work

import (
	"fmt"
)

func redisKeySchedLock(namespace string) string {
	return namespace + ":sched_lock"
}

func redisKeySchedJobLock(namespace string, jobName string, timestamp int64) string {
	return fmt.Sprintf("%s:sched_lock:%s:%d", namespace, jobName, timestamp)
}

func redisKeyScheduled(namespace string) string {
	return namespace + ":scheduled"
}
