package gate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	koutbox "github.com/keelab/contrib/data/sql/outbox"
	coreoutbox "github.com/keelab/keelith/outbox"
)

type DeliveryClient interface {
	Dispatch(context.Context, Task, ExecutionResult) error
	DispatchProgress(context.Context, Task, string, string) error
}

type outboxDelivery struct {
	runtime *koutbox.Runtime
	db      *sql.DB
	client  DeliveryClient
}

type deliveryEnvelope struct {
	Task          Task            `json:"task"`
	Result        ExecutionResult `json:"result"`
	Progress      string          `json:"progress"`
	ProgressState string          `json:"progress_state"`
}

func NewOutboxDelivery(db *sql.DB, runtime *koutbox.Runtime, client DeliveryClient) (*outboxDelivery, error) {
	if db == nil || runtime == nil || client == nil {
		return nil, fmt.Errorf("gatecore: outbox delivery dependencies are incomplete")
	}
	return &outboxDelivery{db: db, runtime: runtime, client: client}, nil
}

func (d *outboxDelivery) Dispatch(ctx context.Context, task Task, result ExecutionResult) error {
	return d.enqueue(ctx, "channelcore.delivery.final", task.ID+"/final", deliveryEnvelope{Task: task, Result: result})
}

func (d *outboxDelivery) EnqueueFinal(ctx context.Context, tx *sql.Tx, task Task, result ExecutionResult) error {
	return d.enqueueTx(ctx, tx, "channelcore.delivery.final", task.ID+"/final", deliveryEnvelope{Task: task, Result: result})
}

func (d *outboxDelivery) EnqueueProgress(ctx context.Context, tx *sql.Tx, task Task, state, content string) error {
	return d.enqueueTx(ctx, tx, "channelcore.delivery.progress", task.ID+"/progress/"+state, deliveryEnvelope{Task: task, Progress: content, ProgressState: state})
}

func (d *outboxDelivery) DispatchProgress(ctx context.Context, task Task, state, content string) error {
	return d.enqueue(ctx, "channelcore.delivery.progress", task.ID+"/progress/"+state, deliveryEnvelope{Task: task, Progress: content, ProgressState: state})
}

func (d *outboxDelivery) enqueue(ctx context.Context, destination, key string, envelope deliveryEnvelope) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("gatecore: begin channel delivery outbox: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := d.enqueueTx(ctx, tx, destination, key, envelope); err != nil {
		return fmt.Errorf("gatecore: enqueue channel delivery: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("gatecore: commit channel delivery outbox: %w", err)
	}
	return nil
}

func (d *outboxDelivery) enqueueTx(ctx context.Context, tx *sql.Tx, destination, key string, envelope deliveryEnvelope) error {
	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("gatecore: encode channel delivery: %w", err)
	}
	message := coreoutbox.Message{ID: fmt.Sprintf("%s-%d", key, time.Now().UnixNano()), Destination: destination, Key: []byte(key), Payload: payload}
	if err := d.runtime.Enqueue(ctx, tx, message, time.Now().UTC()); err != nil {
		return err
	}
	return nil
}

type DeliveryPublisher struct{ client DeliveryClient }

func NewDeliveryPublisher(client DeliveryClient) (*DeliveryPublisher, error) {
	if client == nil {
		return nil, fmt.Errorf("gatecore: delivery publisher client is nil")
	}
	return &DeliveryPublisher{client: client}, nil
}

func (p *DeliveryPublisher) Publish(ctx context.Context, message coreoutbox.Message) error {
	var envelope deliveryEnvelope
	if err := json.Unmarshal(message.Payload, &envelope); err != nil {
		return fmt.Errorf("gatecore: decode channel delivery outbox: %w", err)
	}
	switch message.Destination {
	case "channelcore.delivery.final":
		return p.client.Dispatch(ctx, envelope.Task, envelope.Result)
	case "channelcore.delivery.progress":
		return p.client.DispatchProgress(ctx, envelope.Task, envelope.ProgressState, envelope.Progress)
	default:
		return fmt.Errorf("gatecore: unsupported channel delivery destination %q", message.Destination)
	}
}
