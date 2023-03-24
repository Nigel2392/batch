package results

type Routine[RET any] interface {
	// Only used internally to send the return value into the channel when done.
	Channel() chan<- RET
	// A way to read from the channel.
	//
	// When using read, the channel will not be closed.
	Read() <-chan RET
	// A way to close the channel used to send the result.
	Close()
}
