package funcs

type Func[ARG, RET any] interface {
	Execute() RET
}

type Function[ARG, RET any] struct {
	Callable func(ARG) RET
	Arg      ARG
}

func New[ARG, RET any](f func(ARG) RET, arg ARG) Func[ARG, RET] {
	return &Function[ARG, RET]{
		Callable: f,
		Arg:      arg,
	}
}

func (b *Function[ARG, RET]) Execute() RET {
	return b.Callable(b.Arg)
}

//	func (b *function[ARG, RET]) AsResult() results.Single[RET] {
//		return results.NewDummy[RET](b.Run())
//	}
