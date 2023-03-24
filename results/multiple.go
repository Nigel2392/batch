package results

type Multiple[RET any] interface {
	// Wait for, and return the results of the given batch.
	//
	// This will automatically close the channel.
	Wait() []RET

	// Length of the batch.
	Len() int

	Routine[RET]
}

// A result returned when batching a task.
//
// This can be used to wait for the result of the task.
type results[RET any] struct {
	results chan RET
	len     int
	ret     []RET
}

// Initialize a new result.
func NewMultiple[RET any](r chan RET, len int) Multiple[RET] {
	return &results[RET]{
		results: r,
		len:     len,
	}
}

// Length of the batch of results.
func (r *results[RET]) Len() int {
	return r.len
}

// Return the channel of the result.
//
// This is used internally to pass the result to the pool.
//
//lint:ignore U1000 this is used inside of the pool!
func (r *results[RET]) Channel() chan<- RET {
	return r.results
}

// A way to read from the channel.
//
// When using read, the channel will not be closed.
func (r *results[RET]) Read() <-chan RET {
	return r.results
}

// Close the result.
//
// This is used to close the channel of the result.
//
// This will be done automatically when calling Wait().
func (r *results[RET]) Close() {
	close(r.results)
}

// Wait for, and return the results of the given batch.
func (r *results[RET]) Wait() []RET {
	if r.ret != nil {
		return r.ret
	}
	defer r.Close()
	var results = make([]RET, 0, r.len)
	for result := range r.results {
		results = append(results, result)
		if len(results) == r.len {
			break
		}
	}
	r.ret = results
	return results
}
