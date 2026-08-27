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
	SendNotification(ctx context.Context, message Notification) (NotificationReceipt, error)
	PrepareInbound(ctx context.Context, message InboundPreparation) (InboundPreparationReceipt, error)
	SendMedia(ctx context.Context, message OutboundMedia) (ReceiptEntity, error)
	SetMediaStore(store MediaDomain)
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

// NotificationController lets a platform preserve its native notification
// semantics (mentions and urgency) while keeping the common receipt contract.
type NotificationController interface {
	SendNotification(context.Context, Notification) (NotificationReceipt, error)
}

// CommandRegistrar registers transport-neutral slash command definitions with
// a platform when the adapter supports native command registration.
type CommandRegistrar interface {
	RegisterCommands(context.Context, []CommandDefinition) error
}

type CommandDefinition struct {
	Name        string
	Description string
	Usage       string
	Aliases     []string
	SubCommands []CommandSubcommand
}

type CommandSubcommand struct {
	Name        string
	Description string
	ArgsUsage   string
}

type MessageEditor interface {
	EditMessage(context.Context, string, string, string) error
}

type LifecycleEditor interface {
	EditMessageWithState(context.Context, string, string, string, string, map[string]string) error
}

const (
	MessageLifecycleProgress = "progress"
	MessageLifecycleFinal    = "final"
	MessageLifecycleFailed   = "failed"
)

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
	GroupTrigger  GroupTriggerPolicy
	RatePerSecond float64
	Burst         int
	QueueSize     int
	MaxRetries    int
}

type GroupTriggerPolicy struct {
	MentionOnly bool
	Prefixes    []string
}
type Sink func(Inbound)

type Inbound struct {
	ChannelID  string
	MessageID  string
	ChatID     string
	SenderID   string
	SenderName string
	Sender     SenderInfo
	Peer       Peer
	Content    string
	Metadata   map[string]string
	Media      []MediaPartEntity
	SessionKey string
	MediaScope string
	ReceivedAt time.Time
}

type SenderInfo struct {
	Platform    string
	PlatformID  string
	CanonicalID string
	Username    string
	DisplayName string
	AvatarURL   string
}

type Peer struct {
	Kind string
	ID   string
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

type Notification struct {
	ChannelID      string
	TargetID       string
	Content        string
	Metadata       map[string]string
	MentionIDs     []string
	MentionAll     bool
	Urgency        string
	IdempotencyKey string
}

type NotificationReceipt struct {
	ReceiptEntity
	InvalidMentionIDs []string
}

type InboundPreparation struct {
	ChannelID          string
	TargetID           string
	MessageID          string
	ReplyToMessageID   string
	PlaceholderContent string
	Reaction           string
}

type InboundPreparationReceipt struct {
	TypingActionID       string
	ReactionActionID     string
	PlaceholderMessageID string
}
