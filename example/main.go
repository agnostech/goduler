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
		DBUri:  "redis-15981.c84.us-east-1-2.ec2.cloud.redislabs.com:15981",
		DBPassword: "zmOg204a6bcIklqTTlEBOnAAyb1imy7V",
		DBType: goduler.Redis,
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
