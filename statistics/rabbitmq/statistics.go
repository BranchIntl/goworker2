package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/benmanns/goworker/interfaces"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RMQStatistics implements the Statistics interface for RabbitMQ
type RMQStatistics struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	namespace string
	options   Options
	mu        sync.RWMutex

	// In-memory storage for statistics (RabbitMQ doesn't persist arbitrary data like Redis)
	workers     map[string]*WorkerData
	globalStats *GlobalStatsData
	queueStats  map[string]*QueueStatsData
}

// WorkerData holds worker information and stats
type WorkerData struct {
	Info       interfaces.WorkerInfo `json:"info"`
	Processed  int64                 `json:"processed"`
	Failed     int64                 `json:"failed"`
	StartTime  time.Time             `json:"start_time"`
	LastJob    time.Time             `json:"last_job"`
	CurrentJob *interfaces.JobInfo   `json:"current_job,omitempty"`
}

// GlobalStatsData holds global statistics
type GlobalStatsData struct {
	TotalProcessed int64 `json:"total_processed"`
	TotalFailed    int64 `json:"total_failed"`
}

// QueueStatsData holds queue-specific statistics
type QueueStatsData struct {
	Name      string `json:"name"`
	Processed int64  `json:"processed"`
	Failed    int64  `json:"failed"`
	Workers   int64  `json:"workers"`
}

// NewStatistics creates a new RabbitMQ statistics backend
func NewStatistics(options Options) *RMQStatistics {
	return &RMQStatistics{
		namespace:   options.Namespace,
		options:     options,
		workers:     make(map[string]*WorkerData),
		globalStats: &GlobalStatsData{},
		queueStats:  make(map[string]*QueueStatsData),
	}
}

// Connect establishes connection to RabbitMQ
func (r *RMQStatistics) Connect(ctx context.Context) error {
	conn, err := amqp.Dial(r.options.URI)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	r.conn = conn
	r.channel = channel

	// Declare exchanges and queues for statistics persistence
	if err := r.setupInfrastructure(); err != nil {
		r.Close()
		return fmt.Errorf("failed to setup infrastructure: %w", err)
	}

	// Start background routines for statistics persistence
	go r.periodicStatsPersistence(ctx)
	go r.handleConnectionEvents()

	return nil
}

// setupInfrastructure creates necessary exchanges and queues for statistics
func (r *RMQStatistics) setupInfrastructure() error {
	// Declare exchange for statistics events
	exchangeName := r.getStatsExchange()
	if err := r.channel.ExchangeDeclare(
		exchangeName,
		"topic",
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,   // arguments
	); err != nil {
		return fmt.Errorf("failed to declare stats exchange: %w", err)
	}

	// Declare queue for statistics persistence
	queueName := r.getStatsQueue()
	if _, err := r.channel.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	); err != nil {
		return fmt.Errorf("failed to declare stats queue: %w", err)
	}

	// Bind queue to exchange
	if err := r.channel.QueueBind(
		queueName,
		"stats.*",
		exchangeName,
		false, // no-wait
		nil,   // arguments
	); err != nil {
		return fmt.Errorf("failed to bind stats queue: %w", err)
	}

	return nil
}

// Close closes the RabbitMQ connection
func (r *RMQStatistics) Close() error {
	var errs []error

	if r.channel != nil {
		if err := r.channel.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close channel: %w", err))
		}
	}

	if r.conn != nil {
		if err := r.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close connection: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}

	return nil
}

// Health checks the RabbitMQ connection health
func (r *RMQStatistics) Health() error {
	if r.conn == nil || r.conn.IsClosed() {
		return fmt.Errorf("connection is closed")
	}

	if r.channel == nil {
		return fmt.Errorf("channel is nil")
	}

	// Try to declare a temporary queue to test connectivity
	_, err := r.channel.QueueDeclare(
		"",    // name (let server choose)
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	return nil
}

// Type returns the statistics backend type
func (r *RMQStatistics) Type() string {
	return "rmq"
}

// RegisterWorker registers a worker
func (r *RMQStatistics) RegisterWorker(ctx context.Context, worker interfaces.WorkerInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.workers[worker.ID] = &WorkerData{
		Info:      worker,
		Processed: 0,
		Failed:    0,
		StartTime: worker.Started,
		LastJob:   time.Time{},
	}

	// Update queue stats
	for _, queue := range worker.Queues {
		if _, exists := r.queueStats[queue]; !exists {
			r.queueStats[queue] = &QueueStatsData{
				Name:      queue,
				Processed: 0,
				Failed:    0,
				Workers:   0,
			}
		}
		r.queueStats[queue].Workers++
	}

	// Publish worker registration event
	return r.publishStatsEvent("stats.worker.registered", map[string]interface{}{
		"worker_id": worker.ID,
		"hostname":  worker.Hostname,
		"pid":       worker.Pid,
		"queues":    worker.Queues,
		"started":   worker.Started,
	})
}

// UnregisterWorker removes a worker
func (r *RMQStatistics) UnregisterWorker(ctx context.Context, workerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	worker, exists := r.workers[workerID]
	if !exists {
		return nil // Worker not registered
	}

	// Update queue stats
	for _, queue := range worker.Info.Queues {
		if queueStat, exists := r.queueStats[queue]; exists {
			queueStat.Workers--
			if queueStat.Workers < 0 {
				queueStat.Workers = 0
			}
		}
	}

	delete(r.workers, workerID)

	// Publish worker unregistration event
	return r.publishStatsEvent("stats.worker.unregistered", map[string]interface{}{
		"worker_id": workerID,
	})
}

// RecordJobStarted records that a job has started
func (r *RMQStatistics) RecordJobStarted(ctx context.Context, job interfaces.JobInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if worker, exists := r.workers[job.WorkerID]; exists {
		worker.CurrentJob = &job
		worker.LastJob = time.Now()
	}

	// Publish job started event
	return r.publishStatsEvent("stats.job.started", map[string]interface{}{
		"job_id":     job.ID,
		"queue":      job.Queue,
		"class":      job.Class,
		"worker_id":  job.WorkerID,
		"started_at": time.Now(),
	})
}

// RecordJobCompleted records successful job completion
func (r *RMQStatistics) RecordJobCompleted(ctx context.Context, job interfaces.JobInfo, duration time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Update worker stats
	if worker, exists := r.workers[job.WorkerID]; exists {
		worker.Processed++
		worker.CurrentJob = nil
		worker.LastJob = time.Now()
	}

	// Update global stats
	r.globalStats.TotalProcessed++

	// Update queue stats
	if queueStat, exists := r.queueStats[job.Queue]; exists {
		queueStat.Processed++
	}

	// Publish job completed event
	return r.publishStatsEvent("stats.job.completed", map[string]interface{}{
		"job_id":       job.ID,
		"queue":        job.Queue,
		"class":        job.Class,
		"worker_id":    job.WorkerID,
		"duration_ms":  duration.Milliseconds(),
		"completed_at": time.Now(),
	})
}

// RecordJobFailed records job failure
func (r *RMQStatistics) RecordJobFailed(ctx context.Context, job interfaces.JobInfo, err error, duration time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Update worker stats
	if worker, exists := r.workers[job.WorkerID]; exists {
		worker.Failed++
		worker.CurrentJob = nil
		worker.LastJob = time.Now()
	}

	// Update global stats
	r.globalStats.TotalFailed++

	// Update queue stats
	if queueStat, exists := r.queueStats[job.Queue]; exists {
		queueStat.Failed++
	}

	// Publish job failed event
	return r.publishStatsEvent("stats.job.failed", map[string]interface{}{
		"job_id":      job.ID,
		"queue":       job.Queue,
		"class":       job.Class,
		"worker_id":   job.WorkerID,
		"error":       err.Error(),
		"duration_ms": duration.Milliseconds(),
		"failed_at":   time.Now(),
	})
}

// GetWorkerStats returns statistics for a specific worker
func (r *RMQStatistics) GetWorkerStats(ctx context.Context, workerID string) (interfaces.WorkerStats, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	worker, exists := r.workers[workerID]
	if !exists {
		return interfaces.WorkerStats{ID: workerID}, nil
	}

	inProgress := int64(0)
	if worker.CurrentJob != nil {
		inProgress = 1
	}

	return interfaces.WorkerStats{
		ID:         workerID,
		Processed:  worker.Processed,
		Failed:     worker.Failed,
		InProgress: inProgress,
		StartTime:  worker.StartTime,
		LastJob:    worker.LastJob,
	}, nil
}

// GetQueueStats returns statistics for a specific queue
func (r *RMQStatistics) GetQueueStats(ctx context.Context, queue string) (interfaces.QueueStats, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Get queue length from RabbitMQ
	length := int64(0)
	if r.channel != nil {
		if q, err := r.channel.QueueInspect(r.getQueueName(queue)); err == nil {
			length = int64(q.Messages)
		}
	}

	queueStat, exists := r.queueStats[queue]
	if !exists {
		return interfaces.QueueStats{
			Name:   queue,
			Length: length,
		}, nil
	}

	return interfaces.QueueStats{
		Name:      queue,
		Length:    length,
		Processed: queueStat.Processed,
		Failed:    queueStat.Failed,
		Workers:   queueStat.Workers,
	}, nil
}

// GetGlobalStats returns global statistics
func (r *RMQStatistics) GetGlobalStats(ctx context.Context) (interfaces.GlobalStats, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	queueStats := make(map[string]interfaces.QueueStats)
	for name, stat := range r.queueStats {
		// Get current queue length
		length := int64(0)
		if r.channel != nil {
			if q, err := r.channel.QueueInspect(r.getQueueName(name)); err == nil {
				length = int64(q.Messages)
			}
		}

		queueStats[name] = interfaces.QueueStats{
			Name:      stat.Name,
			Length:    length,
			Processed: stat.Processed,
			Failed:    stat.Failed,
			Workers:   stat.Workers,
		}
	}

	return interfaces.GlobalStats{
		TotalProcessed: r.globalStats.TotalProcessed,
		TotalFailed:    r.globalStats.TotalFailed,
		ActiveWorkers:  int64(len(r.workers)),
		QueueStats:     queueStats,
	}, nil
}

// Helper methods

func (r *RMQStatistics) getStatsExchange() string {
	return fmt.Sprintf("%sstats", r.namespace)
}

func (r *RMQStatistics) getStatsQueue() string {
	return fmt.Sprintf("%sstats_persistence", r.namespace)
}

func (r *RMQStatistics) getQueueName(queue string) string {
	return fmt.Sprintf("%squeue:%s", r.namespace, queue)
}

// publishStatsEvent publishes a statistics event to RabbitMQ
func (r *RMQStatistics) publishStatsEvent(routingKey string, data map[string]interface{}) error {
	if r.channel == nil {
		return fmt.Errorf("channel not available")
	}

	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	return r.channel.Publish(
		r.getStatsExchange(), // exchange
		routingKey,           // routing key
		false,                // mandatory
		false,                // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			Timestamp:   time.Now(),
		},
	)
}

// periodicStatsPersistence periodically persists statistics to a durable queue
func (r *RMQStatistics) periodicStatsPersistence(ctx context.Context) {
	ticker := time.NewTicker(r.options.StatsPersistInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.persistStats()
		}
	}
}

// persistStats saves current statistics state
func (r *RMQStatistics) persistStats() {
	r.mu.RLock()
	defer r.mu.RUnlock()

	statsSnapshot := map[string]interface{}{
		"timestamp":    time.Now(),
		"workers":      r.workers,
		"global_stats": r.globalStats,
		"queue_stats":  r.queueStats,
	}

	if err := r.publishStatsEvent("stats.snapshot", statsSnapshot); err != nil {
		// Log error but don't fail - statistics are best effort
		fmt.Printf("Failed to persist stats snapshot: %v\n", err)
	}
}

// handleConnectionEvents handles connection close events and attempts reconnection
func (r *RMQStatistics) handleConnectionEvents() {
	if r.conn == nil {
		return
	}

	closeChan := make(chan *amqp.Error)
	r.conn.NotifyClose(closeChan)

	for closeErr := range closeChan {
		if closeErr != nil {
			fmt.Printf("RabbitMQ connection closed: %v\n", closeErr)
			// In a production system, you might want to implement reconnection logic here
		}
	}
}
