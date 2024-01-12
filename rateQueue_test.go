package payloadqueue_test

import (
	"sync"
	"testing"
	"time"

	"github.com/sam-ish/payloadqueue"
)

func TestRateQStart(t *testing.T) {
	t.Run("Start RateQueue with Work function", func(t *testing.T) {
		qb := payloadqueue.RateQueue{
			Tag:               "QueueB",
			RequestsPerSecond: 5,
			Work:              func(pls interface{}) int { return 0 },
		}
		if err := qb.Start(); err != nil {
			t.Errorf("Unexpected error: %s", err.Error())
		}
	})

	t.Run("Start RateQueue with zero requests/second", func(t *testing.T) {
		qb := payloadqueue.RateQueue{
			Tag:               "QueueB",
			RequestsPerSecond: 0,
		}
		if err := qb.Start(); err == nil {
			t.Errorf("Expected error - rateQueues cannot have zero requests/second")
		}
	})

	t.Run("Start RateQueue with no Work function", func(t *testing.T) {
		qb := payloadqueue.RateQueue{
			Tag: "QueueB",
		}
		if err := qb.Start(); err == nil {
			t.Errorf("Expected error - Work function is not supplied")
		}
	})
}

func TestRateQNewPayload(t *testing.T) {
	type job struct {
		Name     string `json:"name"`
		Duration int    `json:"duration"` // milliseconds
	}

	validJob := job{Name: "Alpha", Duration: 10}
	emptyJob := job{}

	t.Run("Test Valid Payload for RateQueue", func(t *testing.T) {
		qb := payloadqueue.RateQueue{
			Tag:  "QueueA",
			Work: func(pls interface{}) int { return 0 },
		}
		if pl := qb.NewPayload(validJob); pl.Data.(job).Name != validJob.Name {
			t.Errorf("Payload Data is not consistent %s expected %s found", validJob.Name, pl.Data.(job).Name)
		}
	})

	t.Run("Test Empty Payload for RateQueue", func(t *testing.T) {
		qb := payloadqueue.RateQueue{
			Tag:  "QueueA",
			Work: func(pls interface{}) int { return 0 },
		}
		if pl := qb.NewPayload(emptyJob); pl.Data.(job).Name != emptyJob.Name {
			t.Errorf("Payload Data is not consistent %s expected %s found", validJob.Name, pl.Data.(job).Name)
		}
	})
}

func TestRateQRunNext(t *testing.T) {
	t.Run("Run 2 payloads at 1 Payload/sec", func(t *testing.T) {
		var runMutex sync.Mutex
		runtimes := 0

		q := &payloadqueue.RateQueue{
			MaxSize:           5,
			RequestsPerSecond: 1,
			Tag:               "QueueA",
			Work: func(pls interface{}) int {
				//increment the runtimes
				runMutex.Lock()
				runtimes += 1
				runMutex.Unlock()
				return 0
			},
		}
		q.Start()

		q.Append(payloadqueue.Payload{Id: "1"})
		q.Append(payloadqueue.Payload{Id: "2"})
		q.Append(payloadqueue.Payload{Id: "3"})
		q.Append(payloadqueue.Payload{Id: "4"})
		//q.Append(payloadqueue.Payload{Id: "5"})
		//q.Append(payloadqueue.Payload{Id: "6"})
		//q.Append(payloadqueue.Payload{Id: "7"})

		// Delay is important to be sure that the Work() goroutine has been called before the assertion
		time.Sleep(2400 * time.Millisecond)
		// Ensure that Run was triggered and the queue was reset
		if runtimes != 2 {
			t.Errorf("Expected runtimes to be 2, got %d", runtimes)
		}
		if q.Size() != 2 {
			t.Errorf("Expected q.Size() to be 2, got %d", q.Size())
		}
		q.Close()
	})

	t.Run("Trigger Run directly: RunNext()", func(t *testing.T) {
		q := &payloadqueue.RateQueue{
			MaxSize:           20,
			Tag:               "QueueA",
			RequestsPerSecond: 1,
			Work:              func(pls interface{}) int { return 0 },
		}
		q.Start()
		q.Append(payloadqueue.Payload{Id: "1"})
		q.Append(payloadqueue.Payload{Id: "2"})
		q.Append(payloadqueue.Payload{Id: "3"})
		q.Append(payloadqueue.Payload{Id: "4"})
		q.Append(payloadqueue.Payload{Id: "3"})
		q.Append(payloadqueue.Payload{Id: "3"})
		// Delay is important to be sure that the Work() goroutine has been called before the assertion
		time.Sleep(900 * time.Millisecond)
		curSize := q.Size()
		q.RunNext()
		q.RunNext()
		time.Sleep(1 * time.Second)
		if q.Size() != curSize-3 {
			t.Errorf("Expected q.Size() to be %d, got %d", curSize-2, q.Size())
		}
		q.Close()
	})
}

func TestRateQAppend(t *testing.T) {
	// Test case 1: Append with non-empty ID
	t.Run("Append with non-empty ID", func(t *testing.T) {
		q := &payloadqueue.RateQueue{
			MaxSize:           10, // Set appropriate values for your rate queue configuration
			RequestsPerSecond: 2,  // Set appropriate values for your rate queue configuration
		}
		p := payloadqueue.Payload{Id: "123" /* other payload fields */}
		err := q.Append(p)

		// Add your assertions here based on the expected behavior
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		// Add more assertions as needed
	})

	// Test case 2: Append with an empty ID
	t.Run("Append with empty ID", func(t *testing.T) {
		q := &payloadqueue.RateQueue{
			MaxSize:           10, // Set appropriate values for your rate queue configuration
			RequestsPerSecond: 2,  // Set appropriate values for your rate queue configuration
		}
		p := payloadqueue.Payload{Id: "" /* other payload fields */}
		err := q.Append(p)

		// Add your assertions here based on the expected behavior
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		// Add more assertions as needed
	})

	// Test case 3: Append when the queue is full
	t.Run("Append when the queue is full", func(t *testing.T) {
		q := &payloadqueue.RateQueue{
			MaxSize:           2,  // Set appropriate values for your rate queue configuration
			RequestsPerSecond: 10, // Set appropriate values for your rate queue configuration
		}
		p1 := payloadqueue.Payload{Id: "1" /* other payload fields */}
		p2 := payloadqueue.Payload{Id: "2" /* other payload fields */}
		p3 := payloadqueue.Payload{Id: "3" /* other payload fields */}
		q.Append(p1)
		q.Append(p2)
		err := q.Append(p3)
		if err == nil {
			t.Error("Expected error when appending to a full queue, got nil")
		}
		if q.Size() != 2 {
			t.Errorf("Expected Size() to be 2, got %d", q.Size())
		}
	})
}

func TestRateQPauseAndRestart(t *testing.T) {
	t.Run("Pause & Restart", func(t *testing.T) {
		var runMutex sync.Mutex
		runtimes := 0

		q := &payloadqueue.RateQueue{
			MaxSize:           5,
			RequestsPerSecond: 2,
			Tag:               "QueueA",
			Work: func(pls interface{}) int {
				//increment the runtimes
				runMutex.Lock()
				runtimes += 1
				runMutex.Unlock()
				return 0
			},
		}

		q.Append(payloadqueue.Payload{Id: "1"})
		q.Append(payloadqueue.Payload{Id: "2"})
		q.Append(payloadqueue.Payload{Id: "3"})
		q.Append(payloadqueue.Payload{Id: "4"})
		q.Append(payloadqueue.Payload{Id: "5"})

		q.Start()
		q.Pause()
		time.Sleep(2 * time.Second)
		if runtimes > 0 {
			t.Errorf("Pause Failed: Expected runtimes to be 0, got %d", runtimes)
		}
		q.Restart()
		time.Sleep(2 * time.Second)

		if runtimes <= 3 {
			t.Errorf("Expected runtimes to be > 2, got %d", runtimes)
		}
		q.Close()
	})
}
