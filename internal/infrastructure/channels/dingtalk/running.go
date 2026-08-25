package dingtalk

func (c *Channel) Running() bool { return c.running.Load() }
