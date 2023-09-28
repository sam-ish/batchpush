package payloadqueue

import "math/rand"

type Payload struct {
	id   string
	Data interface{}
}

// work to be implemented by the consumer to handle the batched (array) payload
type workHandler func([]interface{}) int
type rateWorkHandler func(interface{}) int

// eventFeed to pass information/verbose to the client for handling
type eventFeed func(string)

func defaultTag(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
