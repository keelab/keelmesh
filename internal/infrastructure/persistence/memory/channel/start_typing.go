package channel

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/keelab/keelmesh/internal/domain"
)

func (r *Repository) StartTyping(ctx context.Context, channelID, targetID string) (string, error) {
	channel, err := r.Get(channelID)
	if err != nil {
		return "", err
	}
	controller, ok := channel.(domain.TypingController)
	if !ok {
		return "", fmt.Errorf("%w: channel %q does not support typing", ErrUnsupported, channelID)
	}
	stop, err := controller.StartTyping(ctx, targetID)
	if err != nil {
		return "", err
	}
	id := r.newActionID("typing")
	r.mu.Lock()
	r.actions[id] = actionEntry{stop: stop, createdAt: time.Now()}
	r.mu.Unlock()
	return id, nil
}

func (r *Repository) newActionID(kind string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	return fmt.Sprintf("%s-%d-%d", strings.TrimSpace(kind), time.Now().UnixNano(), r.nextID)
}
