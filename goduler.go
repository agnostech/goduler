package goduler

import (
	"context"
	"encoding/json"
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
	DBType     string
	DBUri      string
	DBPassword string
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
		redisClient := redis.NewClient(&redis.Options{
			Addr:     config.DBUri,
			Password: config.DBPassword,
		})
		goduler.redis = redisClient
	}

	return goduler, nil
}

func (goduler *Goduler) Define(jobName string, jobFunction interface{}) {
	goduler.definitions[jobName] = jobFunction
}

func (goduler *Goduler) Schedule(config *job.JobConfig, scheduleTime time.Time, jobData ...interface{}) error {

	newJob := &job.Job{
		Config:      config,
		RedisClient: goduler.redis,
	}

	newJob.Config.JobFunction = goduler.definitions[newJob.Config.JobName]
	goduler.jobs[config.UniqueId] = newJob

	newJob.Schedule(scheduleTime, jobData)

	err := goduler.saveJob(newJob)

	if err != nil {
		return err
	}

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

func (goduler *Goduler) saveJob(saveJob *job.Job) error {

	err := saveJob.Save()

	if err != nil {
		return err
	}

	return nil
}

func (goduler *Goduler) stop() {

	for _, scheduledJob := range goduler.jobs {
		if !scheduledJob.Config.IsDone {
			scheduledJob.Cancel()
		}
	}

}

func (goduler *Goduler) start() error {

	jobsData := goduler.redis.HGetAll(context.Background(), "goduler")

	if err := jobsData.Err(); err != nil {
		return err
	}
	result, _ := jobsData.Result()

	for _, value := range result {
		config := &job.JobConfig{}

		newJob := &job.Job{
			Config: config,
		}

		err := json.Unmarshal([]byte(value), config)
		if err != nil {
			return err
		}

		newJob.Config.JobFunction = goduler.definitions[newJob.Config.JobName]
		goduler.jobs[config.UniqueId] = newJob

		if !newJob.Config.IsDone {
			newJob.Schedule(config.ScheduleTime, config.JobParameters)
		}
	}

	return nil
}
