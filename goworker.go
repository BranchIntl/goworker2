package goworker

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	rmqbroker "github.com/benmanns/goworker/pkg/brokers/rabbitmq"
	redisbroker "github.com/benmanns/goworker/pkg/brokers/redis"
	"github.com/benmanns/goworker/pkg/core"
	"github.com/benmanns/goworker/pkg/interfaces"
	"github.com/benmanns/goworker/pkg/registry"
	"github.com/benmanns/goworker/pkg/serializers/resque"
	"github.com/benmanns/goworker/pkg/serializers/sneakers"
	rmqstats "github.com/benmanns/goworker/pkg/statistics/rabbitmq"
	redisstats "github.com/benmanns/goworker/pkg/statistics/redis"
	"github.com/cihub/seelog"
)

var (
	logger         seelog.LoggerInterface
	engine         interfaces.Engine
	globalRegistry interfaces.Registry
	initMutex      sync.Mutex
	initialized    bool
)

// BrokerType represents the type of broker to use
type BrokerType string

const (
	BrokerTypeRedis    BrokerType = "redis"
	BrokerTypeRabbitMQ BrokerType = "rabbitmq"
)

// WorkerSettings holds the configuration
type WorkerSettings struct {
	// Common settings
	BrokerType     BrokerType
	QueuesString   string
	Queues         queuesFlag
	IntervalFloat  float64
	Interval       intervalFlag
	Concurrency    int
	Connections    int
	ExitOnComplete bool
	IsStrict       bool
	UseNumber      bool

	// Redis-specific settings
	RedisURI       string
	RedisNamespace string
	SkipTLSVerify  bool
	TLSCertPath    string

	// RabbitMQ-specific settings
	RabbitMQURI string
	Exchange     string
	ExchangeType string
	PrefetchCount int
}

var workerSettings WorkerSettings

// Backward compatibility types
type queuesFlag []string
type intervalFlag time.Duration

func (q *queuesFlag) Set(value string) error {
	for _, queueAndWeight := range strings.Split(value, ",") {
		if queueAndWeight == "" {
			continue
		}

		// Simple implementation - ignore weights for now
		queue := strings.Split(queueAndWeight, "=")[0]
		if queue != "" {
			*q = append(*q, queue)
		}
	}

	if len(*q) == 0 {
		return fmt.Errorf("you must specify at least one queue")
	}
	return nil
}

func (q *queuesFlag) String() string {
	return fmt.Sprint(*q)
}

func (i *intervalFlag) Set(value string) error {
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return err
	}
	*i = intervalFlag(time.Duration(f * float64(time.Second)))
	return nil
}

func (i *intervalFlag) SetFloat(value float64) error {
	*i = intervalFlag(time.Duration(value * float64(time.Second)))
	return nil
}

func (i *intervalFlag) String() string {
	return fmt.Sprint(*i)
}

// Initialize flags
func init() {
	// Common flags
	flag.StringVar((*string)(&workerSettings.BrokerType), "broker", "redis", "broker type: redis or rabbitmq")
	flag.StringVar(&workerSettings.QueuesString, "queues", "", "a comma-separated list of queues")
	flag.Float64Var(&workerSettings.IntervalFloat, "interval", 5.0, "sleep interval when no jobs are found")
	flag.IntVar(&workerSettings.Concurrency, "concurrency", 25, "the maximum number of concurrently executing jobs")
	flag.IntVar(&workerSettings.Connections, "connections", 2, "the maximum number of connections to the broker")
	flag.BoolVar(&workerSettings.ExitOnComplete, "exit-on-complete", false, "exit when the queue is empty")
	flag.BoolVar(&workerSettings.UseNumber, "use-number", false, "use json.Number instead of float64 when decoding numbers in JSON")

	// Redis-specific flags
	redisProvider := os.Getenv("REDIS_PROVIDER")
	var redisEnvURI string
	if redisProvider != "" {
		redisEnvURI = os.Getenv(redisProvider)
	} else {
		redisEnvURI = os.Getenv("REDIS_URL")
	}
	if redisEnvURI == "" {
		redisEnvURI = "redis://localhost:6379/"
	}
	flag.StringVar(&workerSettings.RedisURI, "redis-uri", redisEnvURI, "the URI of the Redis server")
	flag.StringVar(&workerSettings.RedisNamespace, "redis-namespace", "resque:", "the Redis namespace")
	flag.StringVar(&workerSettings.TLSCertPath, "tls-cert", "", "path to a custom CA cert")
	flag.BoolVar(&workerSettings.SkipTLSVerify, "insecure-tls", false, "skip TLS validation")

	// RabbitMQ-specific flags
	rmqEnvURI := os.Getenv("RABBITMQ_URL")
	if rmqEnvURI == "" {
		rmqEnvURI = "amqp://guest:guest@localhost:5672/"
	}
	flag.StringVar(&workerSettings.RabbitMQURI, "rabbitmq-uri", rmqEnvURI, "the URI of the RabbitMQ server")
	flag.StringVar(&workerSettings.Exchange, "exchange", "goworker", "the RabbitMQ exchange name")
	flag.StringVar(&workerSettings.ExchangeType, "exchange-type", "direct", "the RabbitMQ exchange type")

	globalRegistry = registry.NewRegistry()

	// Set default broker type
	workerSettings.BrokerType = BrokerTypeRabbitMQ
}

// Init initializes the goworker process
func Init() error {
	initMutex.Lock()
	defer initMutex.Unlock()

	if !initialized {
		var err error
		logger, err = seelog.LoggerFromWriterWithMinLevel(os.Stdout, seelog.InfoLvl)
		if err != nil {
			return err
		}

		if err := flags(); err != nil {
			return err
		}

		// Create broker and stats based on type
		var broker interfaces.Broker
		var stats interfaces.Statistics
		var serializer interfaces.Serializer

		switch workerSettings.BrokerType {
		case BrokerTypeRedis:
			serializer := resque.NewSerializer()
			serializer.SetUseNumber(workerSettings.UseNumber)
			broker, stats, err = createRedisBrokerAndStats(serializer)
		case BrokerTypeRabbitMQ:
			serializer := sneakers.NewSerializer()
			serializer.SetUseNumber(workerSettings.UseNumber)
			broker, stats, err = createRabbitMQBrokerAndStats(serializer)
		default:
			return fmt.Errorf("unsupported broker type: %s", workerSettings.BrokerType)
		}

		if err != nil {
			return fmt.Errorf("failed to create broker: %w", err)
		}

		// Create engine
		engine = core.NewEngine(
			broker,
			stats,
			globalRegistry,
			serializer,
			core.WithConcurrency(workerSettings.Concurrency),
			core.WithQueues(workerSettings.Queues),
			core.WithPollInterval(time.Duration(workerSettings.Interval)),
			core.WithExitOnComplete(workerSettings.ExitOnComplete),
			core.WithStrictQueues(workerSettings.IsStrict),
		)

		initialized = true
	}
	return nil
}

// createRedisBrokerAndStats creates Redis broker and statistics
func createRedisBrokerAndStats(serializer interfaces.Serializer) (interfaces.Broker, interfaces.Statistics, error) {
	brokerOpts := redisbroker.Options{
		URI:            workerSettings.RedisURI,
		Namespace:      workerSettings.RedisNamespace,
		MaxConnections: workerSettings.Connections,
		UseTLS:         workerSettings.SkipTLSVerify,
		TLSCertPath:    workerSettings.TLSCertPath,
	}

	statsOpts := redisstats.Options{
		URI:            workerSettings.RedisURI,
		Namespace:      workerSettings.RedisNamespace,
		MaxConnections: workerSettings.Connections,
		UseTLS:         workerSettings.SkipTLSVerify,
		TLSCertPath:    workerSettings.TLSCertPath,
	}

	broker := redisbroker.NewBroker(brokerOpts, serializer)
	stats := redisstats.NewStatistics(statsOpts)

	return broker, stats, nil
}

// createRabbitMQBrokerAndStats creates RabbitMQ broker and statistics
func createRabbitMQBrokerAndStats(serializer interfaces.Serializer) (interfaces.Broker, interfaces.Statistics, error) {
	brokerOpts := rmqbroker.Options{
		URI: workerSettings.RabbitMQURI,
		PrefetchCount: workerSettings.PrefetchCount,
		Exchange:      workerSettings.Exchange,
		ExchangeType:  workerSettings.ExchangeType,
	}

	statsOpts := rmqstats.DefaultOptions()

	broker := rmqbroker.NewBroker(brokerOpts, serializer)
	stats := rmqstats.NewStatistics(statsOpts)

	return broker, stats, nil
}

// Work starts the goworker process
func Work() error {
	err := Init()
	if err != nil {
		return err
	}
	defer Close()

	ctx := context.Background()

	// Handle signals
	quit := signals()
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		<-quit
		cancel()
	}()

	// Start engine
	if err := engine.Start(ctx); err != nil {
		return err
	}

	// Wait for completion
	<-ctx.Done()

	return engine.Stop()
}

// Close cleans up resources
func Close() {
	initMutex.Lock()
	defer initMutex.Unlock()

	if initialized && engine != nil {
		engine.Stop()
		initialized = false
	}
}

// Register registers a goworker worker function
func Register(class string, worker interfaces.WorkerFunc) {
	globalRegistry.Register(class, worker)
}

// SetSettings sets worker settings
func SetSettings(settings WorkerSettings) {
	workerSettings = settings
}

// Backward compatibility types

// Job represents a job to be processed (backward compatibility~)
type Job struct {
	Queue   string
	Payload Payload
}

// Payload represents the job payload (backward compatibility)
type Payload struct {
	Class string        `json:"class"`
	Args  []interface{} `json:"args"`
}

// Enqueue adds a job to the queue
func Enqueue(job *Job) error {
	if err := Init(); err != nil {
		return err
	}

	// Create job using the broker's job creation method
	var newJob interfaces.Job

	switch workerSettings.BrokerType {
	case BrokerTypeRedis:
		newJob = resque.NewJob(job.Queue, job.Payload.Class, job.Payload.Args)
	case BrokerTypeRabbitMQ:
		newJob = sneakers.NewJob(job.Queue, job.Payload.Class, job.Payload.Args)
	default:
		return fmt.Errorf("unsupported broker type: %s", workerSettings.BrokerType)
	}

	return engine.Enqueue(newJob)
}

// Namespace returns the namespace (Redis-specific, returns empty for RabbitMQ)
func Namespace() string {
	if workerSettings.BrokerType == BrokerTypeRedis {
		return workerSettings.RedisNamespace
	}
	return ""
}

// Helper functions

func flags() error {
	if !flag.Parsed() {
		flag.Parse()
	}
	if err := workerSettings.Queues.Set(workerSettings.QueuesString); err != nil {
		return err
	}
	if err := workerSettings.Interval.SetFloat(workerSettings.IntervalFloat); err != nil {
		return err
	}
	workerSettings.IsStrict = strings.IndexRune(workerSettings.QueuesString, '=') == -1

	// Validate broker type
	switch workerSettings.BrokerType {
	case BrokerTypeRedis, BrokerTypeRabbitMQ:
		// Valid
	default:
		return fmt.Errorf("invalid broker type: %s. Must be 'redis' or 'rabbitmq'", workerSettings.BrokerType)
	}

	if !workerSettings.UseNumber && workerSettings.BrokerType == BrokerTypeRedis {
		logger.Warn("== DEPRECATION WARNING ==")
		logger.Warn("  Currently, encoding/json decodes numbers as float64.")
		logger.Warn("  This can cause numbers to lose precision as they are read from the queue.")
		logger.Warn("  Set the -use-number flag to use json.Number when decoding numbers and remove this warning.")
	}

	return nil
}
