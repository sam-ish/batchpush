package main

import (
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
	Name     string
	Duration int // milliseconds
}

func Datahandler(pls []interface{}) int {
	time.Sleep(3 * time.Second)
	return 0
}

func Print(s string) {
	log.Println(s)
}
