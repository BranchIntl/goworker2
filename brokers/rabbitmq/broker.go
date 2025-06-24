package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/benmanns/goworker/core"
	"github.com/benmanns/goworker/errors"
	"github.com/benmanns/goworker/job"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQBroker implements the Broker interface for RabbitMQ
type RabbitMQBroker struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	options    Options
	serializer core.Serializer
	queues     map[string]bool // Track declared queues
}

// NewBroker creates a new RabbitMQ broker
func NewBroker(options Options, serializer core.Serializer) *RabbitMQBroker {
	return &RabbitMQBroker{
		options:    options,
		serializer: serializer,
		queues:     make(map[string]bool),
	}
}

// Connect establishes connection to RabbitMQ
func (r *RabbitMQBroker) Connect(ctx context.Context) error {
	conn, err := amqp.Dial(r.options.URI)
	if err != nil {
		return errors.NewConnectionError(r.options.URI,
			fmt.Errorf("failed to connect to RabbitMQ: %w", err))
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return errors.NewConnectionError(r.options.URI,
			fmt.Errorf("failed to open channel: %w", err))
	}

	// Set QoS if specified
	if r.options.PrefetchCount > 0 {
		err = ch.Qos(r.options.PrefetchCount, 0, false)
		if err != nil {
			ch.Close()
			conn.Close()
			return errors.NewConnectionError(r.options.URI,
				fmt.Errorf("failed to set QoS: %w", err))
		}
	}

	r.connection = conn
	r.channel = ch

	return nil
}

// Close closes the RabbitMQ connection
func (r *RabbitMQBroker) Close() error {
	if r.channel != nil {
		if err := r.channel.Close(); err != nil {
			return err
		}
	}
	if r.connection != nil {
		return r.connection.Close()
	}
	return nil
}

// Health checks the RabbitMQ connection health
// COMMENT: Not doing a PING here unlike Redis, is this enough. Do we need to open/close a channel to confirm?
func (r *RabbitMQBroker) Health() error {
	if r.connection == nil || r.connection.IsClosed() {
		return errors.ErrNotConnected
	}
	return nil
}

// Type returns the broker type
func (r *RabbitMQBroker) Type() string {
	return "rabbitmq"
}

// Capabilities returns RabbitMQ broker capabilities
func (r *RabbitMQBroker) Capabilities() core.BrokerCapabilities {
	return core.BrokerCapabilities{
		SupportsAck:        true, // RabbitMQ supports ACK/NACK
		SupportsDelay:      true, // Can be implemented with delayed exchange plugin
		SupportsPriority:   true, // RabbitMQ supports message priority
		SupportsDeadLetter: true, // RabbitMQ supports dead letter exchanges
	}
}

// Enqueue adds a job to the queue
func (r *RabbitMQBroker) Enqueue(ctx context.Context, j job.Job) error {
	// Ensure queue exists
	if err := r.ensureQueue(j.GetQueue()); err != nil {
		return errors.NewBrokerError("ensure_queue", j.GetQueue(), err)
	}

	// Serialize job
	data, err := r.serializer.Serialize(j)
	if err != nil {
		return errors.NewSerializationError(r.serializer.GetFormat(),
			fmt.Errorf("serialize job: %w", err))
	}

	// Publish message
	err = r.channel.PublishWithContext(
		ctx,          // context
		"",           // exchange
		j.GetQueue(), // routing key (queue name)
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         data,
			DeliveryMode: amqp.Persistent, // Make message persistent
			Timestamp:    time.Now(),
			MessageId:    j.GetID(),
		})

	if err != nil {
		return errors.NewBrokerError("enqueue", j.GetQueue(), err)
	}

	return nil
}

// Dequeue retrieves a job from the queue
func (r *RabbitMQBroker) Dequeue(ctx context.Context, queue string) (job.Job, error) {
	// Ensure queue exists
	if err := r.ensureQueue(queue); err != nil {
		return nil, errors.NewBrokerError("ensure_queue", queue, err)
	}

	// Get a single message
	delivery, ok, err := r.channel.Get(queue, false) // Don't auto-ack
	if err != nil {
		return nil, errors.NewBrokerError("dequeue", queue, err)
	}

	if !ok {
		return nil, nil // No message available
	}

	// Create metadata
	metadata := job.Metadata{
		Queue:      queue,
		EnqueuedAt: delivery.Timestamp,
	}

	if delivery.MessageId != "" {
		metadata.ID = delivery.MessageId
	}

	// Deserialize job
	j, err := r.serializer.Deserialize(delivery.Body, metadata)
	if err != nil {
		// Reject message if we can't deserialize it
		delivery.Nack(false, false)
		return nil, errors.NewSerializationError(r.serializer.GetFormat(),
			fmt.Errorf("deserialize job: %w", err))
	}

	// Store delivery tag for ACK/NACK
	// Always wrap in RMQJob
	rmqJob := &RMQJob{
		Job:         j,
		deliveryTag: delivery.DeliveryTag,
		channel:     r.channel,
	}

	return rmqJob, nil
}

// Ack acknowledges job completion
func (r *RabbitMQBroker) Ack(ctx context.Context, j job.Job) error {
	if rmqJob, ok := j.(*RMQJob); ok && rmqJob.deliveryTag > 0 {
		return rmqJob.channel.Ack(rmqJob.deliveryTag, false)
	}
	return nil
}

// Nack rejects a job and optionally requeues it
func (r *RabbitMQBroker) Nack(ctx context.Context, j job.Job, requeue bool) error {
	if rmqJob, ok := j.(*RMQJob); ok && rmqJob.deliveryTag > 0 {
		return rmqJob.channel.Nack(rmqJob.deliveryTag, false, requeue)
	}
	return nil
}

// CreateQueue creates a new queue
func (r *RabbitMQBroker) CreateQueue(ctx context.Context, name string, options core.QueueOptions) error {
	args := amqp.Table{}

	// Set message TTL if specified
	if options.MessageTTL > 0 {
		args["x-message-ttl"] = int64(options.MessageTTL / time.Millisecond)
	}

	// Set dead letter exchange if specified
	if options.DeadLetterQueue != "" {
		args["x-dead-letter-exchange"] = ""
		args["x-dead-letter-routing-key"] = options.DeadLetterQueue
	}

	// Set max retries if specified
	if options.MaxRetries > 0 {
		args["x-max-retries"] = options.MaxRetries
	}

	_, err := r.channel.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		args,  // arguments
	)

	if err != nil {
		return errors.NewBrokerError("create_queue", name, err)
	}

	r.queues[name] = true
	return nil
}

// DeleteQueue deletes a queue
func (r *RabbitMQBroker) DeleteQueue(ctx context.Context, name string) error {
	_, err := r.channel.QueueDelete(name, false, false, false)
	if err != nil {
		return errors.NewBrokerError("delete_queue", name, err)
	}

	delete(r.queues, name)
	return nil
}

// QueueExists checks if a queue exists
func (r *RabbitMQBroker) QueueExists(ctx context.Context, name string) (bool, error) {
	_, err := r.channel.QueueDeclarePassive(name, true, false, false, false, nil)
	if err != nil {
		// Queue doesn't exist
		return false, nil
	}
	return true, nil
}

// QueueLength returns the number of jobs in a queue
func (r *RabbitMQBroker) QueueLength(ctx context.Context, name string) (int64, error) {
	state, err := r.channel.QueueInspect(name)
	if err != nil {
		return 0, errors.NewBrokerError("queue_length", name, err)
	}
	return int64(state.Messages), nil
}

// ensureQueue makes sure a queue is declared
func (r *RabbitMQBroker) ensureQueue(name string) error {
	if r.queues[name] {
		return nil // Already declared
	}

	_, err := r.channel.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return err
	}

	r.queues[name] = true
	return nil
}

// RMQJob wraps a job with RabbitMQ-specific delivery information
type RMQJob struct {
	job.Job
	deliveryTag uint64
	channel     *amqp.Channel
}
