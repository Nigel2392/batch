package batch

type channelControl struct {
	cancel chan struct{}
	done   chan struct{}
}

func newChannelControl() *channelControl {
	return &channelControl{
		cancel: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (c *channelControl) Cancel() {
	select {
	case <-c.cancel:
	default:
		close(c.cancel)
	}
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}

func (c *channelControl) Done() <-chan struct{} {
	select {
	case <-c.cancel:
		return c.cancel
	default:
		return c.done
	}
}
