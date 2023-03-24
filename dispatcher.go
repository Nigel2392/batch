package batch

import (
	"fmt"
	"strings"
	"sync"

	"github.com/Nigel2392/batch/funcs"
	"github.com/Nigel2392/batch/results"
)

type Dispatcher[KEY comparable, ARG, RET any] interface {
	// Get a batch from the dispatcher.
	//
	// If the batch does not exist, it will be created.
	Get(batchKey KEY, sliceSize int) Batch[ARG, RET]

	// Add a batch to the dispatcher.
	//
	// If the batch already exists, it will be overwritten.
	AddBatch(batchKey KEY, batch Batch[ARG, RET])

	// Add a task to the batch.
	//
	// This task can be provided with its own arguments.
	Add(batchKey KEY, task func(ARG) RET, arg ARG)

	// Add many tasks to the batch.
	//
	// All tasks will get the arguments provided to AddMany.
	AddMany(batchKey KEY, task []func(ARG) RET, arg ARG)

	// Add Funcs to the batch.
	//
	// This is a shortcut for AddMany.
	AddFuncs(batchKey KEY, funcs ...funcs.Func[ARG, RET])

	// Execute the supplied tasks, and return the
	// and return the results.Multiple[RET] containing a channel for the results.
	//
	// The amount of tasks that can be executed at once is determined by the amount of workers.
	Run(batchKey KEY, workers, chanBufferSize Size) (results.Multiple[RET], ChannelControl)

	// Return the underlying map of batches.
	Batches() BatchMap[KEY, ARG, RET]

	// Remove a batch from the dispatcher.
	//
	// If the batch does not exist, nothing will happen.
	Remove(batchKey KEY)
}

type BatchMap[KEY comparable, ARG, RET any] map[KEY]Batch[ARG, RET]

func (b BatchMap[KEY, ARG, RET]) String() string {
	var bld strings.Builder
	bld.WriteString("BatchMap:\n")
	for k, v := range b {
		bld.WriteString(fmt.Sprintf("%v: %v\n", k, v))
	}
	return bld.String()
}

type dispatcher[KEY comparable, ARG, RET any] struct {
	batches BatchMap[KEY, ARG, RET]
	mu      *sync.Mutex
}

func NewDispatcher[KEY comparable, ARG, RET any]() Dispatcher[KEY, ARG, RET] {
	var d = &dispatcher[KEY, ARG, RET]{
		batches: make(BatchMap[KEY, ARG, RET]),
		mu:      &sync.Mutex{},
	}

	return d
}

func (b *dispatcher[KEY, ARG, RET]) Get(batchKey KEY, sliceSize int) Batch[ARG, RET] {
	b.mu.Lock()
	var _b, ok = b.batches[batchKey]
	if !ok || _b == nil {
		_b = New[ARG, RET](sliceSize)
		b.batches[batchKey] = _b
	}
	b.mu.Unlock()
	return _b
}

func (b *dispatcher[KEY, ARG, RET]) Add(batchKey KEY, f func(ARG) RET, arg ARG) {
	var _b = b.Get(batchKey, 1)
	_b.Add(f, arg)
}

func (b *dispatcher[KEY, ARG, RET]) AddMany(batchKey KEY, f []func(ARG) RET, arg ARG) {
	var _b = b.Get(batchKey, len(f))
	_b.AddMany(f, arg)
}

func (b *dispatcher[KEY, ARG, RET]) AddFuncs(batchKey KEY, f ...funcs.Func[ARG, RET]) {
	var _b = b.Get(batchKey, len(f))
	_b.AddFuncs(f...)
}

func (b *dispatcher[KEY, ARG, RET]) AddBatch(batchKey KEY, batch Batch[ARG, RET]) {
	b.mu.Lock()
	b.batches[batchKey] = batch
	b.mu.Unlock()
}

func (b *dispatcher[KEY, ARG, RET]) Remove(batchKey KEY) {
	b.mu.Lock()
	delete(b.batches, batchKey)
	b.mu.Unlock()
}

func (b *dispatcher[KEY, ARG, RET]) Run(batchKey KEY, workers, chanBufferSize Size) (results.Multiple[RET], ChannelControl) {
	var _b = b.Get(batchKey, 0)
	if _b == nil {
		panic("batch not found")
	}
	return _b.Run(workers, chanBufferSize)
}

func (b *dispatcher[KEY, ARG, RET]) Batches() BatchMap[KEY, ARG, RET] {
	return b.batches
}
