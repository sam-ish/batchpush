# Payload Queue
This library provides 2 Queues: BufferQueue and RateQueue. BufferQueue (the generic) allows you queue data/structs for future work based on queue size or age in the queue. RateQueues allow you to process requests/data in a steady speed/state irrespective of the speed of generation.

It is designed to be lightweight, efficient and easy to use.

Free feel to make any suggestions for improvements/optimizations.

# Simple usage: 
There are simple code examples in the [examples folder](./examples/bufferQueue/). Below is a sample usage:

```
import (
	plq "github.com/sam-ish/payloadqueue"
)

func main() {
	q := plq.Queue{
		Tag:       "QueueName",
		Work:      Datahandler, // your handler for the queued data
		MaxSize:   150,
		MaxAge:    3,
	}
	q.Start() // start queuing
  // Create and append the data-struct to the queue
  qb.Append(qb.NewPayload(struct{
				Name string
			}{
				Name: "DataB",
			})

   // Call the close on exit
   q.Close()
}

// Datahandler to act on the queued data
func Datahandler(data []interface{}) int {
	// ..do meaningful work on the data
	return 0 // zero is success
}
```

