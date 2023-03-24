package batch

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/Nigel2392/batch/funcs"
	"github.com/Nigel2392/batch/results"
)

// The size type used inside the Batch interface.
//
// This is a uint to make sure the size is always positive,
// as it might be used in channels and slices.
type Size uint32

// Maximum buffer size for the pool of executing funcs.
//
// This is to prevent the pool from using too much memory.
//
// Feel free to edit this variable.
var MaxSafeBuffer Size = 16384

// Maximum amount of workers for the pool of executing funcs.
//
// This is to prevent running too many goroutines at once.
//
// Feel free to edit this variable.
var MaxSafeWorkers Size = getMaxWorkers(512) // 512 * logical cpu's

// A channel control is used to control the execution of the batch.
// It can be used to cancel the execution of the batch.
//
// It can also be used to check if the batch has been fully executed.
type ChannelControl interface {
	// Cancel the execution of the batch.
	//
	// All tasks in the buffer will be canceled.
	//
	// Already running tasks will be run to completion.
	Cancel()

	// Check if the batch has been fully executed or canceled.
	Done() <-chan struct{}
}

// A batch of functions
//
// This batch can be used to execute multiple tasks at once.
//
// The tasks can be either scheduled, or executed immediately.
//
// The results are returned in a separate interface,
// containing a channel to read/wait for the the results.
type Batch[ARG, RET any] interface {
	// Execute the supplied tasks, and return the
	// and return the results.Multiple[RET] containing a channel for the results.
	//
	// The amount of tasks that can be executed at once is determined by the amount of workers.
	Run(workers, chanBufferSize Size) (results.Multiple[RET], ChannelControl)

	// Execute the supplied tasks,
	// and return the results.Multiple[RET] containing a channel for the results.
	//
	// The tasks will be executed at the given time.
	//
	// Tasks scheduled for the past will be executed immediately.
	RunAt(workers, chanBufferSize Size, t time.Time) (results.Multiple[RET], ChannelControl)

	// Add a task to the batch.
	//
	// This task can be provided with its own arguments.
	Add(task func(ARG) RET, arg ARG)

	// Add many tasks to the batch.
	//
	// All tasks will get the arguments provided to AddMany.
	AddMany(task []func(ARG) RET, arg ARG)

	// Add Funcs to the batch.
	//
	// This is a shortcut for AddMany.
	AddFuncs(funcs ...funcs.Func[ARG, RET])
}

// Our implementation of the Batch interface.
type batchRunner[ARG, RET any] struct {
	// If the batch is unsafe, it will disable the check for
	// the maximum amount of workers and buffer size.
	Unsafe bool
	funcs  []funcs.Func[ARG, RET]
	mu     *sync.Mutex
}

// Create a new batch runner.
//
// This batch runner can be used to execute multiple tasks at once.
//
// The amount of tasks that can be executed at once is determined by the amount of workers.
func New[ARG, RET any](initialSliceSize int) Batch[ARG, RET] {
	var b = &batchRunner[ARG, RET]{
		funcs: make([]funcs.Func[ARG, RET], 0, initialSliceSize),
		mu:    &sync.Mutex{},
	}

	return b
}

// Add a task to the batch.
//
// The task will be executed in parallel with other tasks.
func (b *batchRunner[ARG, RET]) Add(f func(ARG) RET, arg ARG) {
	b.funcs = append(b.funcs, &funcs.Function[ARG, RET]{Callable: f, Arg: arg})
}

// Add many tasks to the batch with the same arguments.
func (b *batchRunner[ARG, RET]) AddMany(f []func(ARG) RET, arg ARG) {
	if len(f) > 1 && cap(b.funcs) < len(b.funcs)+len(f) {
		// Grow the slice to the correct size.
		var newSlice = make([]funcs.Func[ARG, RET], len(b.funcs), len(b.funcs)+len(f))
		copy(newSlice, b.funcs)
		b.funcs = newSlice
	}
	for _, task := range f {
		b.funcs = append(b.funcs, &funcs.Function[ARG, RET]{Callable: task, Arg: arg})
	}
}

// Add Funcs to the batch.
//
// This is a shortcut for AddMany.
func (b *batchRunner[ARG, RET]) AddFuncs(f ...funcs.Func[ARG, RET]) {
	if len(f) == 0 {
		panic("no funcs provided")
	}
	if cap(b.funcs) < len(b.funcs)+len(f) {
		var newSlice = make([]funcs.Func[ARG, RET], len(b.funcs)+len(f))
		copy(newSlice, b.funcs)
		copy(newSlice[len(b.funcs):], f)
		b.funcs = newSlice
		return
	}
	b.funcs = append(b.funcs, f...)
}

// Execute the supplied tasks, and return the results.
func (b *batchRunner[ARG, RET]) Run(workers, chanBufferSize Size) (results.Multiple[RET], ChannelControl) {

	// Check if the batch contains any tasks.
	if len(b.funcs) == 0 {
		panic("this batch contains no tasks")
	}

	// Initialize the batch for running.
	workers, funcQueue, resultQueue, results := b.initializeQueue(workers, chanBufferSize, len(b.funcs))

	// Initialize a new channel controller.
	var cc = newChannelControl()

	// Execute the tasks.
	b.startWorkers(workers, funcQueue, resultQueue, b.funcs, cc)

	return results, cc
}

func (b *batchRunner[ARG, RET]) initializeQueue(workers, chanBufferSize Size, resultQueueSize int) (Size, chan funcs.Func[ARG, RET], chan RET, results.Multiple[RET]) {
	workers, chanBufferSize = b.checkWorkersAndBufferSize(workers, chanBufferSize)
	var funcQueue, resultQueue, results = b.createQueues(chanBufferSize, resultQueueSize)
	return workers, funcQueue, resultQueue, results
}

// Execute the supplied tasks, and return the results.
//
// The task will be executed at the given time.
func (b *batchRunner[ARG, RET]) RunAt(workers, chanBufferSize Size, t time.Time) (results.Multiple[RET], ChannelControl) {

	// If the time is in the past, execute the tasks immediately.
	if time.Now().After(t) {
		return b.Run(workers, chanBufferSize)
	}

	// Calculate the duration until the given time.
	var d = time.Until(t)

	// Check if the batch is empty.
	if len(b.funcs) == 0 {
		panic("this batch contains no tasks")
	}

	// Initialize a new channel controller.
	var cc = newChannelControl()

	// Initialize the batch for running.
	workers, funcQueue, resultQueue, results := b.initializeQueue(workers, chanBufferSize, len(b.funcs))
	// This will make sure that they're not modified while waiting.
	var tasks = b.funcs
	// Start the batch asynchronously.
	go func(funcs []funcs.Func[ARG, RET]) {
		select {
		// If the batch is cancelled, return.
		case <-cc.cancel:
			return
			// Sleep until the given time.
		case <-time.After(d):
		}
		// Execute the tasks.
		b.startWorkers(workers, funcQueue, resultQueue, funcs, cc)
	}(tasks)
	return results, cc
}

// Initialize the queues for the given amount of workers and buffer size.
func (b *batchRunner[ARG, RET]) createQueues(chanBufferSize Size, resultQueueSize int) (chan funcs.Func[ARG, RET], chan RET, results.Multiple[RET]) {
	var funcQueue = make(chan funcs.Func[ARG, RET], chanBufferSize)
	var resultQueue = make(chan RET, len(b.funcs))
	var results = results.NewMultiple(resultQueue, len(b.funcs))
	return funcQueue, resultQueue, results
}

// Initialize the workers and start the batch.
func (b *batchRunner[ARG, RET]) startWorkers(workers Size, funcQueue chan funcs.Func[ARG, RET], resultQueue chan RET, funcs []funcs.Func[ARG, RET], cc *channelControl) {
	// Start the workers.
	for i := Size(0); i < workers; i++ {
		go b.worker(funcQueue, resultQueue, cc)
	}
	go func() {
		defer close(funcQueue)
		for _, f := range funcs {
			select {
			case <-cc.cancel:
				return
			default:
				funcQueue <- f
			}
		}
	}()
}

// Initialize a new worker for the pool, with the given function queue and result queue.
func (b *batchRunner[ARG, RET]) worker(funcQueue chan funcs.Func[ARG, RET], resultQueue chan RET, cc *channelControl) {
	for f := range funcQueue {
		select {
		case <-cc.cancel:
			return
		default:
			resultQueue <- f.Execute()
		}
	}
	// Send a signal after finishing by closing the finished channel.
	b.mu.Lock()
	select {
	case <-cc.done:
	default:
		close(cc.done)
	}
	b.mu.Unlock()
}

// Check if the amount of workers and buffer size is valid.
// If not, panic or set the value.
func (b *batchRunner[ARG, RET]) checkWorkersAndBufferSize(workers, chanBufferSize Size) (Size, Size) {
	if workers <= 0 {
		workers = 1
	}

	if chanBufferSize <= 0 {
		chanBufferSize = 1
	}

	var funcLen = Size(len(b.funcs))

	if !b.Unsafe {
		if workers > funcLen {
			workers = funcLen
		}

		if chanBufferSize > funcLen {
			chanBufferSize = funcLen
		}

		if workers > MaxSafeWorkers {
			panicCount("workers", MaxSafeWorkers, workers)
		}

		if chanBufferSize > MaxSafeBuffer {
			panicCount("buffer size", MaxSafeBuffer, chanBufferSize)
		}
	}
	return workers, chanBufferSize
}

func panicCount(n string, max, current Size) {
	panic(fmt.Errorf("%s of %d is too large (max: %d)! use batchRunner.Unsafe to override this flag", n, current, max))
}

func getMaxWorkers(mult int) Size {
	var cpuCount = runtime.NumCPU()
	if cpuCount > 1 {
		return Size(cpuCount * mult)
	}
	return Size(mult)
}
