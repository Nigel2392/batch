package results

// A result returned when executing/batching a task.
//
// This can be used to wait for the result of the task.
type Single[RET any] interface {
	// Wait for, and return the result of the given task.
	//
	// This will automatically close the channel.
	Wait() RET

	Routine[RET]
}

// A result returned when executing a task.
type result[RET any] struct {
	resultChan chan RET
}

// Initialize a new result.
func NewSingle[RET any](resultChan chan RET) Single[RET] {
	return &result[RET]{
		resultChan: resultChan,
	}
}

// Return the channel of the result.
//
//lint:ignore U1000 this is used inside of the pool!
func (r *result[RET]) Channel() chan<- RET {
	return r.resultChan
}

// A way to read from the channel.
//
// When using read, the channel will not be closed.
func (r *result[RET]) Read() <-chan RET {
	return r.resultChan
}

// Wait for, and return the result of the given task.
func (r *result[RET]) Wait() RET {
	defer r.Close()
	return <-r.resultChan
}

// Close the result.
//
// This will be done automatically when calling Wait().
func (r *result[RET]) Close() {
	close(r.resultChan)
}
