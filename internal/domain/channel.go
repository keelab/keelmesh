package domain

import (
	"context"
	"io"
	"time"
)

type DeliveryState string

const (
	DeliveryQueued       DeliveryState = "queued"
	DeliverySending      DeliveryState = "sending"
	DeliveryAcknowledged DeliveryState = "acknowledged"
	DeliveryFailed       DeliveryState = "failed"
	DeliveryCancelled    DeliveryState = "cancelled"
)

type ChannelDomain interface {
	Delivery(id string) (DeliveryStatus, bool)
	Get(id string) (Channel, error)
	List() []DefinitionEntity
	LoadMedia(ctx context.Context, ref string) (MediaEntity, error)
	Probe(ctx context.Context, id string) (time.Duration, error)
	Publish(message Inbound)
	ReleaseMedia(ctx context.Context, ref string) error
	Send(ctx context.Context, message Outbound) (ReceiptEntity, error)
	SendMedia(ctx context.Context, message OutboundMedia) (ReceiptEntity, error)
	SetMediaStore(store MediaRepository)
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	StopTyping(id string) bool
	StoreMedia(ctx context.Context, filename, contentType string, content io.Reader) (MediaPartEntity, error)
	Subscribe(ctx context.Context, channelIDs []string) (<-chan Inbound, func(), error)
	CompleteReaction(id string) bool
	EditMessage(ctx context.Context, channelID, targetID, messageID, content, state string, metadata map[string]string) error
	ExpireReaction(id string) bool
	FinishStreamingMessage(ctx context.Context, channelID, targetID, messageID, content string) error
	ReactToMessage(ctx context.Context, channelID, targetID, messageID, reaction string) (string, error)
	SendPlaceholder(ctx context.Context, channelID, targetID, replyTo, content string) (string, error)
	StartStreamingMessage(ctx context.Context, channelID, targetID, replyTo, content string) (string, error)
	StartTyping(ctx context.Context, channelID, targetID string) (string, error)
	UpdateStreamingMessage(ctx context.Context, channelID, targetID, messageID, content string) error
}

type DeliveryStatus struct {
	MessageID  string
	ChannelID  string
	State      DeliveryState
	AcceptedAt time.Time
	UpdatedAt  time.Time
	Error      string
}

// Channel is implemented by each concrete platform adapter. The adapter owns
// platform connections and translates platform messages into channelcore types.
type Channel interface {
	Definition() DefinitionEntity
	Start(context.Context, Sink) error
	Stop(context.Context) error
	Send(context.Context, Outbound) (ReceiptEntity, error)
	Probe(context.Context) error
	Running() bool
}

type MediaChannel interface {
	SendMedia(context.Context, OutboundMedia) (ReceiptEntity, error)
}

type MessageEditor interface {
	EditMessage(context.Context, string, string, string) error
}

type LifecycleEditor interface {
	EditMessageWithState(context.Context, string, string, string, string, map[string]string) error
}

type TypingController interface {
	StartTyping(context.Context, string) (func(), error)
}

type ReactionController interface {
	ReactToMessage(context.Context, string, string, string) (func(), func(), error)
}

type PlaceholderController interface {
	SendPlaceholder(context.Context, string, string, string) (string, error)
}

type StreamingController interface {
	StartStreamingMessage(context.Context, string, string, string) (string, error)
	UpdateStreamingMessage(context.Context, string, string, string) error
	FinishStreamingMessage(context.Context, string, string, string) error
}

type DefinitionEntity struct {
	ID            string
	Kind          string
	Enabled       bool
	Capabilities  []string
	RatePerSecond float64
	Burst         int
	QueueSize     int
	MaxRetries    int
}
type Sink func(Inbound)
type Inbound struct {
	ChannelID  string
	MessageID  string
	ChatID     string
	SenderID   string
	SenderName string
	Content    string
	Metadata   map[string]string
	Media      []MediaPartEntity
	ReceivedAt time.Time
}
type MediaPartEntity struct {
	Type        string
	Ref         string
	Caption     string
	Filename    string
	ContentType string
}

type OutboundMedia struct {
	ID             string
	ChannelID      string
	TargetID       string
	Parts          []MediaPartEntity
	Metadata       map[string]string
	IdempotencyKey string
}
type Outbound struct {
	ID               string
	ChannelID        string
	TargetID         string
	ReplyToMessageID string
	Content          string
	Metadata         map[string]string
	IdempotencyKey   string
}
type ReceiptEntity struct {
	MessageID  string
	AcceptedAt time.Time
	State      DeliveryState
}
