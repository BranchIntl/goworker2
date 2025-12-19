package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/BranchIntl/goworker2/errors"
	"github.com/BranchIntl/goworker2/job"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Serializer interface for serializing and deserializing jobs
type Serializer interface {
	// Serialize converts a job to bytes
	Serialize(j job.Job) ([]byte, error)
	// Deserialize converts bytes to a job
	Deserialize(data []byte, metadata job.Metadata) (job.Job, error)
	// GetFormat returns the serialization format name
	GetFormat() string
}

// QueueOptions for queue creation
type QueueOptions struct {
	// MaxRetries before moving to dead letter queue
	MaxRetries int
	// MessageTTL is how long a message can remain in queue
	MessageTTL time.Duration
	// VisibilityTimeout for message processing
	VisibilityTimeout time.Duration
	// DeadLetterQueue name for failed messages
	DeadLetterQueue string
	// QueueType for defining the type of queue (classic, quorum, stream)
	QueueType string
}

// RabbitMQBroker implements the Broker interface for RabbitMQ
type RabbitMQBroker struct {
	connection     *amqp.Connection
	channel        *amqp.Channel
	options        Options
	serializer     Serializer
	declaredQueues map[string]bool   // Track declared queues
	consumerTags   map[string]string // Track consumer tags
	mu             sync.RWMutex
	notifyClose    chan *amqp.Error
	isConnected    bool
	jobChan        chan<- job.Job
}

// NewBroker creates a new RabbitMQ broker
func NewBroker(options Options, serializer Serializer) *RabbitMQBroker {
	return &RabbitMQBroker{
		options:        options,
		serializer:     serializer,
		declaredQueues: make(map[string]bool),
		consumerTags:   make(map[string]string),
	}
}

// Connect establishes connection to RabbitMQ
func (r *RabbitMQBroker) Connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.connect(ctx)
}

// connect establishes the connection and channel, and sets up monitoring
// This method expects the caller to hold the lock
func (r *RabbitMQBroker) connect(ctx context.Context) error {
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

	// Watch for closing
	r.notifyClose = make(chan *amqp.Error)
	r.connection.NotifyClose(r.notifyClose)
	r.isConnected = true

	// Launch background recovery routine
	// We use a new background context because we want reconnection to persist
	// even if the initial context used for Connect() expires,
	// unless the application itself is shutting down.
	// However, checking if we should use the passed context or a long-lived one.
	// The Start() method blocks, but Connect() returns.
	// Typically, libraries handle this with a long-lived loop.
	if r.options.ReconnectEnabled {
		go r.handleReconnection(context.Background())
	}

	return nil
}

func (r *RabbitMQBroker) handleReconnection(ctx context.Context) {
	for {
		select {
		case err := <-r.notifyClose:
			if err == nil {
				return // Graceful shutdown
			}
			slog.Warn("Connection closed, reconnecting...", "error", err)

			r.mu.Lock()
			r.isConnected = false
			r.mu.Unlock()

			// Retry loop
			for {
				time.Sleep(r.options.ReconnectDelay)

				r.mu.Lock()
				// Check if we are already connected (could happen if multiple routines try, though unlikely here)
				if r.isConnected {
					r.mu.Unlock()
					break
				}

				err := r.connect(ctx)
				r.mu.Unlock()

				if err == nil {
					slog.Info("Reconnected to RabbitMQ")

					// Restart consumers
					if err := r.startConsumers(ctx); err != nil {
						slog.Error("Failed to restart consumers after reconnection", "error", err)
						// If we fail to restart consumers, we should retry connecting/restarting
						// For now, we continue the loop to try again
						continue
					} else {
						slog.Info("Consumers restarted successfully")
						break // Exit the retry loop
					}
				}
				slog.Warn("Reconnect failed", "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
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
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.isConnected || r.connection == nil || r.connection.IsClosed() {
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
func (r *RabbitMQBroker) CreateQueue(ctx context.Context, name string, options QueueOptions) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.channel == nil {
		return errors.ErrNotConnected
	}

	// Build arguments
	args := r.buildQueueArgs(options)

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

	r.declaredQueues[name] = true
	return nil
}

// DeleteQueue deletes a queue
func (r *RabbitMQBroker) DeleteQueue(ctx context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.channel == nil {
		return errors.ErrNotConnected
	}

	_, err := r.channel.QueueDelete(name, false, false, false)
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

// Queues returns the list of queues
func (r *RabbitMQBroker) Queues() []string {
	return r.options.Queues
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
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.channel == nil {
		return nil, errors.ErrNotConnected
	}
	return r.channel, nil
}

// ensureQueue makes sure a queue is declared
func (r *RabbitMQBroker) ensureQueue(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.channel == nil {
		return errors.ErrNotConnected
	}

	if r.declaredQueues[name] {
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

	r.declaredQueues[name] = true
	return nil
}

// Start begins consuming jobs and sending them to the job channel
func (r *RabbitMQBroker) Start(ctx context.Context, jobChan chan<- job.Job) error {
	r.mu.Lock()
	r.jobChan = jobChan
	r.mu.Unlock()

	slog.Info("Starting RabbitMQ consumer", "queues", r.options.Queues)
	if len(r.options.Queues) == 0 {
		return errors.ErrNoQueues
	}

	if err := r.startConsumers(ctx); err != nil {
		return err
	}

	// Keep running until context is cancelled
	<-ctx.Done()
	slog.Info("RabbitMQ consumer stopped")
	close(jobChan)
	return nil
}

func (r *RabbitMQBroker) startConsumers(ctx context.Context) error {
	r.mu.RLock()
	jobChan := r.jobChan
	r.mu.RUnlock()

	if jobChan == nil {
		return nil
	}

	for _, queue := range r.options.Queues {
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

// RMQJob wraps a job with RabbitMQ-specific delivery information
type RMQJob struct {
	job.Job
	deliveryTag uint64
	channel     *amqp.Channel
}
