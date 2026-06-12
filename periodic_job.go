package work

import (
	"time"

	"github.com/robfig/cron/v3"
)

type periodicJob struct {
	spec     string
	name     string
	period   time.Duration
	schedule cron.Schedule
}

func newPeriodicJob(spec string, name string) (*periodicJob, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(spec)
	if err != nil {
		return nil, err
	}

	// Estimate period based on next two runs
	now := time.Now()
	t1 := sched.Next(now)
	t2 := sched.Next(t1)
	period := t2.Sub(t1)

	return &periodicJob{
		spec:     spec,
		name:     name,
		period:   period,
		schedule: sched,
	}, nil
}

func (p *periodicJob) nextRun(t time.Time) time.Time {
	return p.schedule.Next(t)
}
