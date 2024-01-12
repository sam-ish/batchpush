package payloadqueue_test

import (
	"sync"
	"testing"
	"time"

	"github.com/sam-ish/payloadqueue"
)

func TestQueueStart(t *testing.T) {
	t.Run("Start Queue with Work function", func(t *testing.T) {
		qb := payloadqueue.Queue{
			Tag:  "QueueB",
			Work: func(pls []interface{}) int { return 0 },
		}
		if err := qb.Start(); err != nil {
			t.Errorf("Unexpected error: %s", err.Error())
		}
	})

	t.Run("Start Queue with no Work function", func(t *testing.T) {
		qb := payloadqueue.Queue{
			Tag: "QueueB",
		}
		if err := qb.Start(); err == nil {
			t.Errorf("Expected error - Work function is not supplied")
		}
	})
}

func TestQueueNewPayload(t *testing.T) {
	type job struct {
		Name     string `json:"name"`
		Duration int    `json:"duration"` // milliseconds
	}

	validJob := job{Name: "Alpha", Duration: 10}
	emptyJob := job{}

	t.Run("Test Valid Payload for BufferQueue", func(t *testing.T) {
		qb := payloadqueue.Queue{
			Tag:  "QueueA",
			Work: func(pls []interface{}) int { return 0 },
		}
		if pl := qb.NewPayload(validJob); pl.Data.(job).Name != validJob.Name {
			t.Errorf("Payload Data is not consistent %s expected %s found", validJob.Name, pl.Data.(job).Name)
		}
	})

	t.Run("Test Empty Payload for BufferQueue", func(t *testing.T) {
		qb := payloadqueue.Queue{
			Tag:  "QueueA",
			Work: func(pls []interface{}) int { return 0 },
		}
		if pl := qb.NewPayload(emptyJob); pl.Data.(job).Name != emptyJob.Name {
			t.Errorf("Payload Data is not consistent %s expected %s found", validJob.Name, pl.Data.(job).Name)
		}
	})
}

func TestQueueRun(t *testing.T) {
	t.Run("Trigger Run by Append()", func(t *testing.T) {
		var runMutex sync.Mutex
		runtimes := 0

		q := &payloadqueue.Queue{
			MaxSize: 2,   // Set appropriate values for your queue configuration
			MaxAge:  200, // Set appropriate values for your queue configuration
			Tag:     "QueueA",
			Work: func(pls []interface{}) int {
				//increment the runtimes
				runMutex.Lock()
				runtimes += 1
				runMutex.Unlock()
				return 0
			},
		}
		q.Start()

		if err := q.Append(payloadqueue.Payload{Id: "1"}); err != nil {
			t.Errorf("Append had an error: %s", err.Error())
		}
		q.Append(payloadqueue.Payload{Id: "2"}) // fires after
		q.Append(payloadqueue.Payload{Id: "3"})
		q.Append(payloadqueue.Payload{Id: "4"}) // fires after

		// Delay is important to be sure that the Work() goroutine has been called before the assertion
		time.Sleep(1 * time.Second)
		// Ensure that Run was triggered and the queue was reset
		if runtimes != 2 {
			t.Errorf("Expected runtimes to be 2, got %d", runtimes)
		}
		q.Close()
	})

	t.Run("Trigger Run directly: Run()", func(t *testing.T) {
		q := &payloadqueue.Queue{
			MaxSize: 2,   // Set appropriate values for your queue configuration
			MaxAge:  200, // Set appropriate values for your queue configuration
			Tag:     "QueueA",
			Work:    func(pls []interface{}) int { return 0 },
		}
		q.Start()

		err := q.Run([]payloadqueue.Payload{
			{Id: "2"}, {Id: "2"}, {Id: "2"}, {Id: "2"},
		})
		if err != nil {
			t.Errorf("Run() had an error: %s", err.Error())
		}
		q.Close()
	})
}

func TestQueueAppend(t *testing.T) {

	// Test case 1: Append with non-empty ID
	t.Run("Append with non-empty ID", func(t *testing.T) {
		q := &payloadqueue.Queue{
			MaxSize: 10,  // Set appropriate values for your queue configuration
			MaxAge:  200, // Set appropriate values for your queue configuration
			Tag:     "QueueA",
			Work:    func(pls []interface{}) int { return 0 },
		}
		q.Start()
		p := payloadqueue.Payload{Id: "123" /* other payload fields */}
		if err := q.Append(p); err != nil {
			t.Errorf("Append had an error: %s", err.Error())
		}
		// Add your assertions here based on the expected behavior
		if q.Size() != 1 {
			t.Errorf("Expected payloadQueue length to be 1, got %d", q.Size())
		}
		q.Close()
	})

	// Test case 2: Append with an empty ID
	t.Run("Append with empty ID", func(t *testing.T) {
		q := &payloadqueue.Queue{
			MaxSize: 10,  // Set appropriate values for your queue configuration
			MaxAge:  200, // Set appropriate values for your queue configuration
			Tag:     "QueueA",
			Work:    func(pls []interface{}) int { return 0 },
		}
		q.Start()
		p := payloadqueue.Payload{Id: "" /* other payload fields */}
		q.Append(p)

		// Add your assertions here based on the expected behavior
		if q.Size() != 0 {
			t.Errorf("Expected payloadQueue length to be 0, got %d", q.Size())
		}
		q.Close()
	})

	// Test case 3: Append to trigger Run
	t.Run("Append to trigger Run", func(t *testing.T) {
		q := &payloadqueue.Queue{
			MaxSize: 2,   // Set appropriate values for your queue configuration
			MaxAge:  200, // Set appropriate values for your queue configuration
			Tag:     "QueueA",
			Work:    func(pls []interface{}) int { return 0 },
		}
		q.Start()
		p1 := payloadqueue.Payload{Id: "1" /* other payload fields */}
		p2 := payloadqueue.Payload{Id: "2" /* other payload fields */}
		q.Append(p1)
		q.Append(p2)

		// Add your assertions here based on the expected behavior
		// Ensure that Run was triggered and the queue was reset
		if q.Size() != 0 {
			t.Errorf("Expected payloadQueue length to be 0, got %d", q.Size())
		}
		q.Close()
	})
}
