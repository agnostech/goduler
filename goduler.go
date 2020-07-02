package goduler

import (
	"errors"
	"github.com/agnostech/goduler/job"
	"github.com/go-redis/redis/v8"
	"time"
)

const (
	Redis    = "redis"
	Postgres = "postgres"
	MongoDB  = "mongo"
)

type GodulerConfig struct {
	DBType string
	DBUri  string
}

type Goduler struct {
	definitions map[string]interface{}
	jobs        map[interface{}]*job.Job
	config      *GodulerConfig
	redis       *redis.Client
}

func New(config *GodulerConfig) (*Goduler, error) {

	goduler := &Goduler{
		config:      config,
		definitions: make(map[string]interface{}),
		jobs:        make(map[interface{}]*job.Job),
	}

	if config.DBType == Redis {
		parsedUrl, parseErr := redis.ParseURL(config.DBUri)
		if parseErr != nil {
			return nil, errors.New("cannot parse Redis connection URL")
		}
		redisClient := redis.NewClient(parsedUrl)
		goduler.redis = redisClient
	}

	return goduler, nil
}

func (goduler *Goduler) Define(jobName string, jobFunction interface{}) {
	goduler.definitions[jobName] = jobFunction
}

func (goduler *Goduler) Schedule(config *job.JobConfig, scheduleTime time.Time, jobData ...interface{}) error {

	newJob := &job.Job{
		Config: config,
	}

	newJob.Config.JobFunction = goduler.definitions[newJob.Config.JobName]
	goduler.jobs[config.UniqueId] = newJob

	newJob.Schedule(scheduleTime, jobData)

	return nil
}

func (goduler *Goduler) cancel(jobId interface{}) error {

	savedJob, ok := goduler.jobs[jobId]

	if !ok {
		return errors.New("cannot find a saved job by the job id mentioned")
	}

	if savedJob.Config.IsDone {
		//Job is done, nothing to cancel
		return nil
	}

	savedJob.Cancel()

	return nil
}

func (goduler *Goduler) stop() {

	for _, scheduledJob := range goduler.jobs {
		if !scheduledJob.Config.IsDone {
			scheduledJob.Cancel()
		}
	}

}
