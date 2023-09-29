package payloadqueue

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// RateQueue to hold the main application queuing mechanism.
type RateQueue struct {
	Tag               string
	MaxSize           int // Default is 100,000
	RequestsPerSecond int
	Work              rateWorkHandler
	EventFeed         eventFeed
	DiscardOnClose    bool
	payloadQueue      []Payload
	payloadChan       chan Payload
	quitChan          chan bool
	delay             time.Duration
	active            bool
}

// Start to open the queue to receive payload to batch
func (q *RateQueue) Start() error {
	if q.RequestsPerSecond < 1 {
		return errors.New("rateQueues cannot have zero requests/second")
	}
	if q.Work == nil {
		return errors.New("the Work function is not supplied")
	}
	q.delay = time.Duration(1000/q.RequestsPerSecond) * time.Millisecond
	if q.MaxSize == 0 {
		q.MaxSize = 100000
		q.event("MaxSize: Default value of 100 was used")
	}
	if q.RequestsPerSecond > 1000 {
		q.RequestsPerSecond = 1000
		q.event("RequestsPerSecond: Max value of 1000 was used")
	}
	if q.Tag == "" {
		q.Tag = defaultTag(12)
		q.event("Tag: Random value assigned is: " + q.Tag)
	}

	go func() {
		// Check for the max age
		for {
			time.Sleep(q.delay)
			q.RunNext()
		}
	}()

	go func() {
		for {
			select {
			case p := <-q.payloadChan:
				// Payload has been added for queuing
				q.Append(p)

			case <-q.quitChan:
				// We have been asked to stop.
				q.Close()
				return
			}
		}
	}()
	q.event("RateQueue: Started")
	q.active = true
	return nil
}

func (q RateQueue) NewPayload(pl interface{}) Payload {
	u := uuid.New()
	return Payload{
		Id:   u.String(),
		Data: pl,
	}
}

// Run to push the Batch for processing
func (q *RateQueue) RunNext() {
	if len(q.payloadQueue) < 1 || !q.active {
		return
	}
	var pl Payload
	//pl, q.payloadQueue = q.payloadQueue[len(q.payloadQueue)-1], q.payloadQueue[:len(q.payloadQueue)-1]
	pl, q.payloadQueue = q.payloadQueue[0], q.payloadQueue[1:]
	q.event("Pushed [" + pl.Id + "] @ " + time.Now().UTC().String() + ". Result: " + strconv.Itoa(q.Work(pl.Data)))
}

// Append to add a Payload to the queue.
func (q *RateQueue) Append(p Payload) error {
	// Check the conditions for firing the Work()
	// 1. Queue is full
	if len(q.payloadQueue) >= q.MaxSize {
		q.event("Payload " + p.Id + " failed. RateQueue is full")
		return errors.New("Payload " + p.Id + " failed. RateQueue is full. Try again later")
	}
	// Add to the queue
	if p.Id != "" {
		q.payloadQueue = append(q.payloadQueue, p)
		q.event("Payload Queued [id]: " + p.Id)
	}
	return nil
}

// Size to return the number of jobs in the queue.
func (q *RateQueue) Size() int {
	return len(q.payloadQueue)
}

// Pause to return the number of jobs in the queue.
func (q *RateQueue) Pause() {
	q.active = false
}

// Restart to return the number of jobs in the queue.
func (q *RateQueue) Restart() {
	q.active = true
}

// Close to close the channels and wait for Work funcs to quit the execution.
func (q *RateQueue) Close() {
	q.event("Rate Queue: Stopping...")
	if q.payloadChan != nil {
		close(q.payloadChan)
	}
	if q.quitChan != nil {
		close(q.quitChan)
	}
	if !q.DiscardOnClose {
		// Flush all active routines to be completed
		q.delay = 100000
		fmt.Println("Pending Payloads in Queue: " + strconv.Itoa(len(q.payloadQueue)))
		for len(q.payloadQueue) > 0 {
			q.RunNext()
		}
	}
	q.active = false
	q.event("Rate Queue: All Work completed")
}

func (q *RateQueue) event(s string) {
	if q.EventFeed != nil {
		q.EventFeed("[" + q.Tag + "] " + s)
	}
}
