package job

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"
	"reflect"
	"time"
)

const (
	JOB_ONCE   = "JOB ONCE"
	JOB_REPEAT = "JOB REPEAT"
)

type JobConfig struct {
	UniqueId      string   `json:"uniqueId"`
	JobName       string        `json:"jobName"`
	IsDone        bool          `json:"isDone"`
	JobType       string        `json:"jobType"`
	MaxRetries    int           `json:"maxRetries"`
	Retries       int           `json:"retries"`
	JobFunction   interface{}   `json:"-"`
	ScheduleTime  time.Time     `json:"scheduleTime"`
	CronString    string        `json:"cronString"`
	CronId        cron.EntryID  `json:"cronId"`
	JobParameters []interface{} `json:"jobParameters"`
	OneOffTimer   *time.Timer   `json:"-"`
}

type Job struct {
	Config      *JobConfig
	RedisClient *redis.Client
}

func (job *Job) Schedule(jobTime time.Time, jobData []interface{}) {

	job.Config.ScheduleTime = jobTime
	job.Config.JobType = JOB_ONCE
	job.Config.JobParameters = jobData
	job.Config.oneOffTimer = time.NewTimer(time.Duration(jobTime.Second()) * time.Second)

	go func() {
		<-job.Config.oneOffTimer.C

		job.run()

		job.Config.IsDone = true

		if err := job.Save(); err != nil {
			//TODO: handle error
		}

	}()

}

func (job *Job) RepeatEvery(cronScheduler *cron.Cron, cronTime string, jobData []interface{}) error {
	job.Config.JobType = JOB_REPEAT
	job.Config.JobParameters = jobData
	job.Config.CronString = cronTime

	entryId, err := cronScheduler.AddFunc(cronTime, func() {
		job.run()
	})

	if err != nil {
		return err
	}

	job.Config.CronId = entryId

	return nil

}

func (job *Job) run() {

	jobFunction := reflect.ValueOf(job.Config.JobFunction)
	jobParameters := make([]reflect.Value, len(job.Config.JobParameters))

	for index, parameter := range job.Config.JobParameters {
		jobParameters[index] = reflect.ValueOf(parameter)
	}

	defer func() {
		if err := recover(); err != nil {
			if job.Config.Retries < job.Config.MaxRetries {
				jobFunction.Call(jobParameters)
				job.Config.Retries++
			} else {
				//TODO: handle error during function execution
			}
		}
	}()

	jobFunction.Call(jobParameters)
}

func (job *Job) Save() error {

	jobJson, err := job.toJSON()
	if err != nil {
		return err
	}

	cmd := job.RedisClient.HSet(context.Background(), job.Config.JobName, job.Config.UniqueId, jobJson)
	if err := cmd.Err(); err != nil {
		return err
	}

	return nil
}

func (job *Job) removeFromDB() error {
	cmd := job.RedisClient.HDel(context.Background(), job.Config.JobName, job.Config.UniqueId)
	if err := cmd.Err(); err != nil {
		return err
	}

	return nil
}

func (job *Job) Cancel(cronScheduler *cron.Cron) error {
	if job.Config.JobType == JOB_ONCE {
		job.Config.oneOffTimer.Stop()
	} else {
		cronScheduler.Remove(job.Config.CronId)
	}

	if err := job.removeFromDB(); err != nil {
		return err
	}

	return nil
}

func (job *Job) toJSON() (string, error) {
	data, err := json.Marshal(job.Config)
	if err != nil {
		return "", err
	}

	return string(data), nil
}
