package job

import (
	"reflect"
	"time"
)

const (
	JOB_ONCE   = "JOB ONCE"
	JOB_REPEAT = "JOB REPEAT"
)

type JobConfig struct {
	UniqueId      interface{}
	JobName       string
	isDone        bool
	JobType       string
	maxRetries    int
	retries       int
	JobFunction   interface{}
	JobParameters []interface{}
	oneOffTimer   *time.Timer
	repeatTimer   *time.Ticker
}

type Job struct {
	Config *JobConfig
}

func (job *Job) Schedule(jobTime time.Time, jobData []interface{}) {
	job.Config.JobType = JOB_ONCE
	job.Config.JobParameters = jobData
	job.Config.oneOffTimer = time.NewTimer(time.Duration(jobTime.Second()) * time.Second)

	go func() {
		<-job.Config.oneOffTimer.C
		job.run()
		job.Config.isDone = true
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
			if job.Config.retries < job.Config.maxRetries {
				jobFunction.Call(jobParameters)
				job.Config.retries++
			}
		}
	}()

	jobFunction.Call(jobParameters)
}
