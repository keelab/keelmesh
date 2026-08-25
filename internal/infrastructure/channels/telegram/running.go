package telegram

func (c *Channel) Running() bool { return c.running.Load() }
