package main

import (
	"fmt"
	"github.com/agnostech/goduler"
	"github.com/agnostech/goduler/job"
	"time"
)

func task(name string, pvc string) {
	fmt.Println("This function ran bc")
	fmt.Println(name)
	fmt.Println(pvc)
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

		}, time.Now().Add(10 * time.Second), "vishal", "wassup",
	)

	if scheduleErr != nil {
		fmt.Println(scheduleErr)
	}

	time.Sleep(time.Duration(40 * time.Second))

}
