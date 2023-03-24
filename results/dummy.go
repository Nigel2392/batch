package results

type Dummy[RET any] interface {
	// Does not actually wait in a goroutine.
	// This can be used for testing, or when using no goroutines.
	Wait() RET

	// No-op.
	// This is to differentiate between a Dummy and a Single with type casting.
	Dummy()

	// Close() will be a no-op.
	// Channel() chan<- RET will return nil.
	// Read() <-chan RET will return nil.
	Routine[RET] // To conform to the Routine interface for use in pools.
}

func IsDummy[RET any](r Routine[RET]) bool {
	_, ok := r.(Dummy[RET])
	return ok
}

type dummy[ARGS, RET any] struct {
	result RET
	f      func(...ARGS) RET
	args   []ARGS
}

func NewDummy[ARGS, RET any](result RET) Dummy[RET] {
	return &dummy[ARGS, RET]{
		result: result,
	}
}

func NewDummyFunc[ARGS, RET any](f func(...ARGS) RET, args ...ARGS) Dummy[RET] {
	return &dummy[ARGS, RET]{
		f:    f,
		args: args,
	}
}

func (d *dummy[ARGS, RET]) Close() {}

func (d *dummy[ARGS, RET]) Read() <-chan RET { return nil }

func (d *dummy[ARGS, RET]) Channel() chan<- RET { return nil }

func (d *dummy[ARGS, RET]) Dummy() {}

func (d *dummy[ARGS, RET]) Wait() RET {
	if d.f != nil {
		return d.f(d.args...)
	}
	return d.result
}
