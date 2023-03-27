package accumulator

import (
	"sync"
	"time"
)

// An accumulator adds a number of items to a queue and
// flushes the queue when the queue is full or a certain time has passed.
//
// The items can then be processed by a worker with the specified handler.
type Accumulator[T any] struct {
	// The queue for the items.
	Queue []T

	// The channel used to send items to a worker.
	ch chan T

	// The number of items to accumulate before the queue is flushed.
	FlushSize int

	// The time to wait before flushing the queue.
	FlushInterval time.Duration

	// ticker is a ticker which is used to flush the queue.
	ticker *time.Ticker

	// The mutex used to lock the queue.
	mutex *sync.Mutex

	// closeChan is a channel which is closed when the batch is closed.
	closeChan chan struct{}

	// The function which is called when the queue is flushed.
	FlushFunc func(T)
}

// NewAccumulator creates a new accumulator which accumulates items and flushes them when the flush size is reached or the flush interval is reached.
func NewAccumulator[T any](flushSize int, flushInterval time.Duration, flushFunc func(T)) *Accumulator[T] {
	var ch chan T
	if flushSize > 0 {
		ch = make(chan T, flushSize)
	} else {
		ch = make(chan T)
	}
	var a = &Accumulator[T]{
		FlushSize:     flushSize,
		FlushInterval: flushInterval,
		FlushFunc:     flushFunc,
		Queue:         make([]T, 0, flushSize),
		ch:            ch,
		mutex:         &sync.Mutex{},
		closeChan:     make(chan struct{}),
	}
	a.ticker = time.NewTicker(flushInterval)
	go a.worker()
	go a.watcher()
	return a
}

func (a *Accumulator[T]) watcher() {
	for {
		select {
		case <-a.closeChan:
			return
		case <-a.ticker.C:
			a.Flush()
		}
	}
}

func (a *Accumulator[T]) worker() {
	for {
		select {
		case <-a.closeChan:
			return
		case e := <-a.ch:
			a.FlushFunc(e)
		}
	}
}

// Push adds an item to the accumulator.
func (a *Accumulator[T]) Push(item T) {
	a.mutex.Lock()
	a.Queue = append(a.Queue, item)
	var needsFlush bool = len(a.Queue) >= a.FlushSize
	if needsFlush {
		a.mutex.Unlock()
		a.Flush()
	}
	if !needsFlush {
		a.mutex.Unlock()
	}
}

// Flush flushes the queue.
func (a *Accumulator[T]) Flush() {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	var deleted int
	for i, entry := range a.Queue {
		j := i - deleted
		a.ch <- entry
		a.Queue = removeSliceItem(a.Queue, j)
		deleted++
	}

}

// Close closes the accumulator.
func (a *Accumulator[T]) Close() {
	a.Flush()
	close(a.closeChan)
}

func removeSliceItem[T any](slice []T, index int) []T {
	return append(slice[:index], slice[index+1:]...)
}
