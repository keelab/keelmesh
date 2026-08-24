package channelcore

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (r *Runtime) EditMessage(ctx context.Context, channelID, targetID, messageID, content, state string, metadata map[string]string) error {
	channel, err := r.Get(channelID)
	if err != nil {
		return err
	}
	if editor, ok := channel.(LifecycleEditor); ok {
		return editor.EditMessageWithState(ctx, targetID, messageID, content, state, metadata)
	}
	editor, ok := channel.(MessageEditor)
	if !ok {
		return fmt.Errorf("channelcore: channel %q does not support message editing", channelID)
	}
	return editor.EditMessage(ctx, targetID, messageID, content)
}

func (r *Runtime) SendPlaceholder(ctx context.Context, channelID, targetID, replyTo, content string) (string, error) {
	channel, err := r.Get(channelID)
	if err != nil {
		return "", err
	}
	controller, ok := channel.(PlaceholderController)
	if !ok {
		return "", fmt.Errorf("channelcore: channel %q does not support placeholders", channelID)
	}
	return controller.SendPlaceholder(ctx, targetID, replyTo, content)
}

func (r *Runtime) StartTyping(ctx context.Context, channelID, targetID string) (string, error) {
	channel, err := r.Get(channelID)
	if err != nil {
		return "", err
	}
	controller, ok := channel.(TypingController)
	if !ok {
		return "", fmt.Errorf("channelcore: channel %q does not support typing", channelID)
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

func (r *Runtime) StopTyping(id string) bool {
	r.mu.Lock()
	entry, ok := r.actions[id]
	delete(r.actions, id)
	r.mu.Unlock()
	if ok && entry.stop != nil {
		entry.stop()
	}
	return ok
}

func (r *Runtime) ReactToMessage(ctx context.Context, channelID, targetID, messageID, reaction string) (string, error) {
	channel, err := r.Get(channelID)
	if err != nil {
		return "", err
	}
	controller, ok := channel.(ReactionController)
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

func (r *Runtime) applyReaction(id string, complete bool) bool {
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

func (r *Runtime) CompleteReaction(id string) bool { return r.applyReaction(id, true) }

func (r *Runtime) ExpireReaction(id string) bool { return r.applyReaction(id, false) }

func (r *Runtime) StartStreamingMessage(ctx context.Context, channelID, targetID, replyTo, content string) (string, error) {
	channel, err := r.Get(channelID)
	if err != nil {
		return "", err
	}
	controller, ok := channel.(StreamingController)
	if !ok {
		return "", fmt.Errorf("channelcore: channel %q does not support streaming messages", channelID)
	}
	return controller.StartStreamingMessage(ctx, targetID, replyTo, content)
}

func (r *Runtime) UpdateStreamingMessage(ctx context.Context, channelID, targetID, messageID, content string) error {
	channel, err := r.Get(channelID)
	if err != nil {
		return err
	}
	controller, ok := channel.(StreamingController)
	if !ok {
		return fmt.Errorf("channelcore: channel %q does not support streaming messages", channelID)
	}
	return controller.UpdateStreamingMessage(ctx, targetID, messageID, content)
}

func (r *Runtime) FinishStreamingMessage(ctx context.Context, channelID, targetID, messageID, content string) error {
	channel, err := r.Get(channelID)
	if err != nil {
		return err
	}
	controller, ok := channel.(StreamingController)
	if !ok {
		return fmt.Errorf("channelcore: channel %q does not support streaming messages", channelID)
	}
	return controller.FinishStreamingMessage(ctx, targetID, messageID, content)
}

func (r *Runtime) newActionID(kind string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	return fmt.Sprintf("%s-%d-%d", strings.TrimSpace(kind), time.Now().UnixNano(), r.nextID)
}
