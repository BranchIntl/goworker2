package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/BranchIntl/goworker2/core"
	"github.com/BranchIntl/goworker2/errors"
	"github.com/BranchIntl/goworker2/job"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQBroker implements the Broker and Poller interfaces for RabbitMQ
type RabbitMQBroker struct {
	connection     *amqp.Connection
	channel        *amqp.Channel
	options        Options
	serializer     core.Serializer
	declaredQueues map[string]bool   // Track declared queues
	consumerQueues []string          // Queues to consume from
	consumerTags   map[string]string // Track consumer tags
}

// NewBroker creates a new RabbitMQ broker
func NewBroker(options Options, serializer core.Serializer) *RabbitMQBroker {
	return &RabbitMQBroker{
		options:        options,
		serializer:     serializer,
		declaredQueues: make(map[string]bool),
		consumerTags:   make(map[string]string),
	}
}

// NewBrokerWithQueues creates a new RabbitMQ broker with consumer queues
func NewBrokerWithQueues(options Options, serializer core.Serializer, queues []string) *RabbitMQBroker {
	return &RabbitMQBroker{
		options:        options,
		serializer:     serializer,
		declaredQueues: make(map[string]bool),
		consumerQueues: queues,
		consumerTags:   make(map[string]string),
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

// Enqueue adds a job to the queue
func (r *RabbitMQBroker) Enqueue(ctx context.Context, j job.Job) error {
	channel, err := r.getChannel()
	if err != nil {
		return err
	}

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
	err = channel.PublishWithContext(
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
	channel, err := r.getChannel()
	if err != nil {
		return nil, err
	}

	// Ensure queue exists
	if err := r.ensureQueue(queue); err != nil {
		return nil, errors.NewBrokerError("ensure_queue", queue, err)
	}

	// Get a single message
	delivery, ok, err := channel.Get(queue, false) // Don't auto-ack
	if err != nil {
		return nil, errors.NewBrokerError("dequeue", queue, err)
	}

	if !ok {
		return nil, nil // No message available
	}

	// Convert delivery to job using the shared method
	job := r.convertDeliveryToJob(delivery, queue)
	if job == nil {
		return nil, errors.NewSerializationError(r.serializer.GetFormat(),
			fmt.Errorf("failed to convert delivery to job"))
	}

	return job, nil
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
	channel, err := r.getChannel()
	if err != nil {
		return err
	}

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

	_, err = channel.QueueDeclare(
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

	r.declaredQueues[name] = true
	return nil
}

// DeleteQueue deletes a queue
func (r *RabbitMQBroker) DeleteQueue(ctx context.Context, name string) error {
	channel, err := r.getChannel()
	if err != nil {
		return err
	}

	_, err = channel.QueueDelete(name, false, false, false)
	if err != nil {
		return errors.NewBrokerError("delete_queue", name, err)
	}

	delete(r.declaredQueues, name)
	return nil
}

// QueueExists checks if a queue exists
func (r *RabbitMQBroker) QueueExists(ctx context.Context, name string) (bool, error) {
	channel, err := r.getChannel()
	if err != nil {
		return false, err
	}

	_, err = channel.QueueDeclarePassive(name, true, false, false, false, nil)
	if err != nil {
		// Queue doesn't exist
		return false, nil
	}
	return true, nil
}

// QueueLength returns the number of jobs in a queue
func (r *RabbitMQBroker) QueueLength(ctx context.Context, name string) (int64, error) {
	channel, err := r.getChannel()
	if err != nil {
		return 0, err
	}

	queue, err := channel.QueueDeclarePassive(name, true, false, false, false, nil)
	if err != nil {
		return 0, errors.NewBrokerError("queue_length", name, err)
	}
	return int64(queue.Messages), nil
}

// getChannel returns the channel if connected, otherwise returns ErrNotConnected
func (r *RabbitMQBroker) getChannel() (*amqp.Channel, error) {
	if r.channel == nil {
		return nil, errors.ErrNotConnected
	}
	return r.channel, nil
}

// ensureQueue makes sure a queue is declared
func (r *RabbitMQBroker) ensureQueue(name string) error {
	channel, err := r.getChannel()
	if err != nil {
		return err
	}

	if r.declaredQueues[name] {
		return nil // Already declared
	}

	_, err = channel.QueueDeclare(
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

	r.declaredQueues[name] = true
	return nil
}

// Start implements the Poller interface for push-based consumption
func (r *RabbitMQBroker) Start(ctx context.Context, jobChan chan<- job.Job) error {
	slog.Info("Starting RabbitMQ consumer", "queues", r.consumerQueues)

	for _, queue := range r.consumerQueues {
		if err := r.ensureQueue(queue); err != nil {
			return fmt.Errorf("failed to ensure queue %s: %w", queue, err)
		}

		deliveries, err := r.channel.Consume(
			queue, // queue
			"",    // consumer tag (auto-generated)
			false, // auto-ack
			false, // exclusive
			false, // no-local
			false, // no-wait
			nil,   // args
		)
		if err != nil {
			return fmt.Errorf("failed to start consumer for queue %s: %w", queue, err)
		}

		// Store consumer tag for cleanup
		if r.consumerTags == nil {
			r.consumerTags = make(map[string]string)
		}

		go r.handleDeliveries(ctx, queue, deliveries, jobChan)
	}

	// Keep running until context is cancelled
	<-ctx.Done()
	slog.Info("RabbitMQ consumer stopped")
	close(jobChan)
	return nil
}

// handleDeliveries processes incoming messages from RabbitMQ
func (r *RabbitMQBroker) handleDeliveries(ctx context.Context, queue string, deliveries <-chan amqp.Delivery, jobChan chan<- job.Job) {
	for {
		select {
		case <-ctx.Done():
			return
		case delivery, ok := <-deliveries:
			if !ok {
				slog.Warn("Delivery channel closed", "queue", queue)
				return
			}

			// Convert delivery to job
			job := r.convertDeliveryToJob(delivery, queue)
			if job != nil {
				select {
				case <-ctx.Done():
					// Put job back on queue
					if err := delivery.Nack(false, true); err != nil {
						slog.Error("Failed to nack job during shutdown", "error", err)
					}
					return
				case jobChan <- job:
					slog.Debug("Job sent to workers", "class", job.GetClass())
				}
			}
		}
	}
}

// convertDeliveryToJob converts an AMQP delivery to a Job
func (r *RabbitMQBroker) convertDeliveryToJob(delivery amqp.Delivery, queue string) job.Job {
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
		if nackErr := delivery.Nack(false, false); nackErr != nil {
			slog.Error("Failed to nack message after deserialization error", "error", nackErr)
		}
		slog.Error("Failed to deserialize job", "error", err)
		return nil
	}

	// Wrap in RMQJob for proper ACK/NACK handling
	rmqJob := &RMQJob{
		Job:         j,
		deliveryTag: delivery.DeliveryTag,
		channel:     r.channel,
	}

	return rmqJob
}

// SetConsumerQueues sets the queues that this broker will consume from
func (r *RabbitMQBroker) SetConsumerQueues(queues []string) {
	r.consumerQueues = queues
}

// RMQJob wraps a job with RabbitMQ-specific delivery information
type RMQJob struct {
	job.Job
	deliveryTag uint64
	channel     *amqp.Channel
}
