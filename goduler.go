package goduler

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/agnostech/goduler/job"
	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"
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
	Timezone   string
}

type Goduler struct {
	definitions map[string]interface{}
	jobs        map[interface{}]*job.Job
	config      *GodulerConfig
	redis       *redis.Client
	godulerCron *cron.Cron
}

func New(config *GodulerConfig) (*Goduler, error) {

	goduler := &Goduler{
		config:      config,
		definitions: make(map[string]interface{}),
		jobs:        make(map[interface{}]*job.Job),
	}

	if timezone := config.Timezone; timezone != "" {
		location, err := time.LoadLocation(timezone)
		if err != nil {
			return nil, errors.New("invalid timezone")
		}
		goduler.godulerCron = cron.New(cron.WithLocation(location))
	} else {
		goduler.godulerCron = cron.New()
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

func (goduler *Goduler) createJob(config *job.JobConfig, jobData ...interface{}) *job.Job {
	newJob := &job.Job{
		Config:      config,
		RedisClient: goduler.redis,
	}

	newJob.Config.JobFunction = goduler.definitions[newJob.Config.JobName]
	goduler.jobs[config.UniqueId] = newJob

	return newJob
}

func (goduler *Goduler) Schedule(config *job.JobConfig, scheduleTime time.Time, jobData ...interface{}) error {

	newJob := goduler.createJob(config, jobData)
	newJob.Schedule(scheduleTime, jobData)

	if err := goduler.saveJob(newJob); err != nil {
		return err
	}

	return nil
}

func (goduler *Goduler) RepeatEvery(config *job.JobConfig, cronTime string, jobData ...interface{}) error {

	newJob := goduler.createJob(config, jobData)

	if err := newJob.RepeatEvery(goduler.godulerCron, cronTime, jobData); err != nil {
		return err
	}

	if err := goduler.saveJob(newJob); err != nil {
		return err
	}

	return nil
}

func (goduler *Goduler) Cancel(jobId interface{}) error {

	savedJob, ok := goduler.jobs[jobId]

	if !ok {
		return errors.New("cannot find a saved job by the job id mentioned")
	}

	if savedJob.Config.IsDone {
		//Job is done, nothing to cancel
		return nil
	}

	if err := savedJob.Cancel(goduler.godulerCron); err != nil {
		return err
	}

	delete(goduler.jobs, jobId)

	return nil
}

func (goduler *Goduler) CancelAll(jobName string) error {

	cmd := goduler.redis.Del(context.Background(), jobName)

	if err := cmd.Err(); err != nil {
		return err
	}

	for _, savedJob := range goduler.jobs {
		if savedJob.Config.JobName == jobName {
			if savedJob.Config.JobType == job.JOB_ONCE {
				savedJob.Config.OneOffTimer.Stop()
			} else {
				goduler.godulerCron.Remove(savedJob.Config.CronId)
			}
			delete(goduler.jobs, savedJob.Config.UniqueId)
		}
	}

	return nil
}

func (goduler *Goduler) saveJob(saveJob *job.Job) error {

	if err := saveJob.Save(); err != nil {
		return err
	}

	return nil
}

func (goduler *Goduler) Stop() error {

	for _, scheduledJob := range goduler.jobs {
		if !scheduledJob.Config.IsDone {
			if err := scheduledJob.Cancel(goduler.godulerCron); err != nil {
				return err
			}
		}
	}

	goduler.godulerCron.Stop()

	return nil

}

func (goduler *Goduler) Start() error {

	for key := range goduler.definitions {
		jobsData := goduler.redis.HGetAll(context.Background(), key)

		if err := jobsData.Err(); err != nil {
			return err
		}
		result, _ := jobsData.Result()

		for _, value := range result {
			config := &job.JobConfig{}

			if err := json.Unmarshal([]byte(value), config); err != nil {
				return err
			}

			newJob := goduler.createJob(config, config.JobParameters)

			if newJob.Config.JobType == job.JOB_ONCE {
				if !newJob.Config.IsDone {
					newJob.Schedule(config.ScheduleTime, config.JobParameters)
				}
			} else {
				if err := newJob.RepeatEvery(goduler.godulerCron, config.CronString, config.JobParameters); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
