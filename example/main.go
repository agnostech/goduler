package main

import (
	"fmt"
	"github.com/agnostech/goduler"
	"github.com/agnostech/goduler/job"
	"time"
)

func task() {
	fmt.Println("This function ran bc")
}

func main() {

	scheduler, err := goduler.New(&goduler.GodulerConfig{
		DBUri:  "",
		DBType: goduler.MongoDB,
	})
	if err != nil {
		print(err)
	}

	scheduler.Define("test-job", task)
	scheduleErr := scheduler.Schedule(
		&job.JobConfig{
			UniqueId: 1,
			JobName: "test-job",

		}, time.Now().Add(1*time.Minute), "name",
	)

	if scheduleErr != nil {
		fmt.Println(scheduleErr)
	}

}
