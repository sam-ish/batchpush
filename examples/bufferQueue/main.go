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

	q := plq.Queue{
		Tag:       "QueueA",
		Work:      Datahandler,
		EventFeed: Print,
		MaxSize:   150,
		MaxAge:    3,
	}
	q.Start()

	qb := plq.Queue{
		Tag:       "QueueB",
		Work:      Datahandler,
		EventFeed: Print,
	}
	qb.Start()

	time.Sleep(2 * time.Second)

	go func() {
		for i := 0; i < 50; i++ {
			q.Append(q.NewPayload(Job{
				Name:     "DataA" + strconv.Itoa(i*2),
				Duration: 1,
			}),
			)
			time.Sleep(200 * time.Millisecond)
		}
	}()

	go func() {
		for i := 0; i < 50; i++ {
			qb.Append(qb.NewPayload(struct {
				Name string
			}{
				Name: "DataB",
			}),
			)
		}
	}()

	time.Sleep(60 * time.Second)
	q.Close()
	Print("Finished")
}

type Job struct {
	Name     string `json:"name"`
	Duration int    `json:"duration"` // milliseconds
}

func Datahandler(pls []interface{}) int {
	jobs := make([]Job, 0)
	bodyBytes, _ := json.Marshal(pls)
	json.Unmarshal(bodyBytes, &jobs)

	for _, v := range jobs {
		fmt.Println("Got: " + v.Name)
	}
	return 0
}

func Print(s string) {
	log.Println(s)
}
