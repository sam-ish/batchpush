package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	plq "github.com/sam-ish/payloadqueue"
)

func main() {

	q := plq.RateQueue{
		Tag:               "RateQueueA",
		Work:              Datahandler,
		EventFeed:         Print,
		MaxSize:           50,
		RequestsPerSecond: 5,
	}
	q.Start()

	go func() {
		for i := 0; i < 60; i++ {
			q.Append(q.NewPayload(Job{
				Name:     "DataA0" + strconv.Itoa(i*2),
				Duration: (i + 1) * 2,
			}),
			)
		}
	}()

	time.Sleep(3 * time.Second)
	q.Close()
	Print("Finished")
}

type Job struct {
	Name     string `json:"name"`
	Duration int    `json:"duration"` // milliseconds
}

func Datahandler(pl interface{}) int {
	job := Job{}
	bodyBytes, _ := json.Marshal(pl)
	json.Unmarshal(bodyBytes, &job)

	fmt.Println(time.Now().String()+": Got: "+job.Name, job.Duration)
	return 0
}

func Print(s string) {
	log.Println(s)
}
