package channel

func (r *Repository) CompleteReaction(id string) bool {
	return r.applyReaction(id, true)
}

func (r *Repository) applyReaction(id string, complete bool) bool {
	r.mu.Lock()
	action, ok := r.reactions[id]
	delete(r.reactions, id)
	r.mu.Unlock()
	if !ok {
		return false
	}
	if complete {
		if action.complete != nil {
			action.complete()
		}
	} else if action.expire != nil {
		action.expire()
	}
	return true
}
