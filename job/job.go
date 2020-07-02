package job

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"reflect"
	"time"
)

const (
	JOB_ONCE   = "JOB ONCE"
	JOB_REPEAT = "JOB REPEAT"
)

type JobConfig struct {
	UniqueId      interface{}   `json:"uniqueId"`
	JobName       string        `json:"jobName"`
	IsDone        bool          `json:"isDone"`
	JobType       string        `json:"jobType"`
	MaxRetries    int           `json:"maxRetries"`
	Retries       int           `json:"retries"`
	JobFunction   interface{}   `json:"-"`
	ScheduleTime  time.Time     `json:"scheduleTime"`
	JobParameters []interface{} `json:"jobParameters"`
	oneOffTimer   *time.Timer   `json:"-"`
	repeatTimer   *time.Ticker  `json:"-"`
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

		//TODO: remove the job from Redis

	}()

}

func (job *Job) RepeatEvery(cronTime string, jobData []interface{}) {
	job.Config.JobType = JOB_REPEAT
	job.Config.JobParameters = jobData
	job.Config.repeatTimer = time.NewTicker(1 * time.Second)

	go func() {
		for timer := range job.Config.repeatTimer.C {
			_ = timer
			//TODO: Writing repeating job logic based on the cron time
		}
	}()
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

	cmd := job.RedisClient.HSet(context.Background(), "goduler", job.Config.UniqueId, jobJson)
	if err := cmd.Err(); err != nil {
		return err
	}

	return nil
}

func (job *Job) Cancel() {
	if job.Config.JobType == JOB_ONCE {
		job.Config.oneOffTimer.Stop()
	} else {
		job.Config.repeatTimer.Stop()
	}
}

func (job *Job) toJSON() (string, error) {
	data, err := json.Marshal(job.Config)
	if err != nil {
		return "", err
	}

	return string(data), nil
}
