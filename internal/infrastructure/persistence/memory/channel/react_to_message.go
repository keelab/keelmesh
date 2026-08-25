package channel

import (
	"context"
	"fmt"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) ReactToMessage(ctx context.Context, channelID, targetID, messageID, reaction string) (string, error) {
	channel, err := r.Get(channelID)
	if err != nil {
		return "", err
	}
	controller, ok := channel.(domain.ReactionController)
	if !ok {
		return "", fmt.Errorf("channelcore: channel %q does not support reactions", channelID)
	}
	complete, expire, err := controller.ReactToMessage(ctx, targetID, messageID, reaction)
	if err != nil {
		return "", err
	}
	id := r.newActionID("reaction")
	r.mu.Lock()
	r.reactions[id] = reactionAction{complete: complete, expire: expire, createdAt: time.Now()}
	r.mu.Unlock()
	return id, nil
}
