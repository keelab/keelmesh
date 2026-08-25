package channel

func (r *Repository) ExpireReaction(id string) bool {
	return r.applyReaction(id, false)
}
