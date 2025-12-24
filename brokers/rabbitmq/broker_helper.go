package rabbitmq

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// buildQueueArgs creates the AMQP arguments table from options
func (r *RabbitMQBroker) buildQueueArgs(options QueueOptions) amqp.Table {
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
		// For Quorum Queues (and generally good practice in newer RMQ), map to x-delivery-limit
		args["x-delivery-limit"] = options.MaxRetries
	}

	// Set queue type if specified
	if options.QueueType != "" {
		args["x-queue-type"] = options.QueueType
	}

	return args
}
