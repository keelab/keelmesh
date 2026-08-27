package channel

import (
	"fmt"
	"strings"

	"github.com/keelab/keelmesh/internal/domain"
	"github.com/keelab/keelmesh/internal/platform/clock"
)

func (r *Repository) Publish(message domain.Inbound) {
	channel, err := r.Get(message.ChannelID)
	if err != nil {
		return
	}
	message, ok := applyGroupTrigger(channel.Definition().GroupTrigger, message)
	if !ok {
		return
	}
	normalizeInbound(&message, channel.Definition().Kind)
	if message.ReceivedAt.IsZero() {
		message.ReceivedAt = clock.UTC()
	}
	r.mu.RLock()
	forwardQueue := r.forwardQueue
	var dropped uint64
	for _, sub := range r.subs {
		if len(sub.channels) > 0 {
			if _, ok := sub.channels[message.ChannelID]; !ok {
				continue
			}
		}
		select {
		case sub.queue <- message:
		default:
			dropped++
		}
	}
	r.mu.RUnlock()
	if dropped > 0 {
		r.inboundDropped.Add(dropped)
	}
	if forwardQueue != nil {
		select {
		case forwardQueue <- message:
		default:
			r.forwardDropped.Add(1)
		}
	}
}

func normalizeInbound(message *domain.Inbound, kind string) {
	if message == nil {
		return
	}
	if message.Sender.Platform == "" {
		message.Sender.Platform = kind
	}
	if message.Sender.PlatformID == "" {
		message.Sender.PlatformID = message.SenderID
	}
	if message.Sender.CanonicalID == "" && message.Sender.Platform != "" && message.Sender.PlatformID != "" {
		message.Sender.CanonicalID = message.Sender.Platform + ":" + message.Sender.PlatformID
	}
	if message.Sender.DisplayName == "" {
		message.Sender.DisplayName = message.SenderName
	}
	if message.Peer.ID == "" {
		message.Peer.ID = message.ChatID
	}
	if message.Peer.Kind == "" {
		message.Peer.Kind = "direct"
		if message.Metadata["scope"] == "group" {
			message.Peer.Kind = "group"
		}
	}
	if message.SessionKey == "" {
		message.SessionKey = fmt.Sprintf("%s:%s:%s", kind, message.ChatID, message.SenderID)
	}
	if message.MediaScope == "" {
		message.MediaScope = fmt.Sprintf("%s:%s:%s", kind, message.ChatID, message.MessageID)
	}
}

func applyGroupTrigger(policy domain.GroupTriggerPolicy, message domain.Inbound) (domain.Inbound, bool) {
	if message.Metadata["scope"] != "group" {
		return message, true
	}
	if message.Metadata["mentioned"] == "true" {
		message.Content = strings.TrimSpace(message.Content)
		return message, true
	}
	if policy.MentionOnly {
		return domain.Inbound{}, false
	}
	if len(policy.Prefixes) == 0 {
		message.Content = strings.TrimSpace(message.Content)
		return message, true
	}
	for _, prefix := range policy.Prefixes {
		if prefix != "" && strings.HasPrefix(message.Content, prefix) {
			message.Content = strings.TrimSpace(strings.TrimPrefix(message.Content, prefix))
			return message, true
		}
	}
	return domain.Inbound{}, false
}
