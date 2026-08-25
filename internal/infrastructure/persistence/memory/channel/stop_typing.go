package channel

func (r *Repository) StopTyping(id string) bool {
	r.mu.Lock()
	entry, ok := r.actions[id]
	delete(r.actions, id)
	r.mu.Unlock()
	if ok && entry.stop != nil {
		entry.stop()
	}
	return ok
}
