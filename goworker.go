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

	redisbroker "github.com/benmanns/goworker/pkg/brokers/redis"
	"github.com/benmanns/goworker/pkg/core"
	"github.com/benmanns/goworker/pkg/interfaces"
	"github.com/benmanns/goworker/pkg/registry"
	json_serializer "github.com/benmanns/goworker/pkg/serializers/resque"
	redisstats "github.com/benmanns/goworker/pkg/statistics/redis"
	"github.com/cihub/seelog"
	"github.com/gomodule/redigo/redis"
)

var (
	logger         seelog.LoggerInterface
	engine         interfaces.Engine
	globalRegistry interfaces.Registry
	initMutex      sync.Mutex
	initialized    bool
)

// WorkerSettings holds the configuration (backward compatibility)
type WorkerSettings struct {
	QueuesString   string
	Queues         queuesFlag
	IntervalFloat  float64
	Interval       intervalFlag
	Concurrency    int
	Connections    int
	URI            string
	Namespace      string
	ExitOnComplete bool
	IsStrict       bool
	UseNumber      bool
	SkipTLSVerify  bool
	TLSCertPath    string
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

// Initialize flags (backward compatibility)
func init() {
	flag.StringVar(&workerSettings.QueuesString, "queues", "", "a comma-separated list of Resque queues")
	flag.Float64Var(&workerSettings.IntervalFloat, "interval", 5.0, "sleep interval when no jobs are found")
	flag.IntVar(&workerSettings.Concurrency, "concurrency", 25, "the maximum number of concurrently executing jobs")
	flag.IntVar(&workerSettings.Connections, "connections", 2, "the maximum number of connections to the Redis database")

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
	flag.StringVar(&workerSettings.URI, "uri", redisEnvURI, "the URI of the Redis server")

	flag.StringVar(&workerSettings.Namespace, "namespace", "resque:", "the Redis namespace")
	flag.StringVar(&workerSettings.TLSCertPath, "tls-cert", "", "path to a custom CA cert")
	flag.BoolVar(&workerSettings.ExitOnComplete, "exit-on-complete", false, "exit when the queue is empty")
	flag.BoolVar(&workerSettings.UseNumber, "use-number", false, "use json.Number instead of float64 when decoding numbers in JSON. will default to true soon")
	flag.BoolVar(&workerSettings.SkipTLSVerify, "insecure-tls", false, "skip TLS validation")

	globalRegistry = registry.NewRegistry()
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

		// Create components
		brokerOpts := redisbroker.Options{
			URI:            workerSettings.URI,
			Namespace:      workerSettings.Namespace,
			MaxConnections: workerSettings.Connections,
			UseTLS:         workerSettings.SkipTLSVerify,
			TLSCertPath:    workerSettings.TLSCertPath,
		}

		statsOpts := redisstats.Options{
			URI:            workerSettings.URI,
			Namespace:      workerSettings.Namespace,
			MaxConnections: workerSettings.Connections,
			UseTLS:         workerSettings.SkipTLSVerify,
			TLSCertPath:    workerSettings.TLSCertPath,
		}

		serializer := json_serializer.NewSerializer()
		serializer.SetUseNumber(workerSettings.UseNumber)

		broker := redisbroker.NewBroker(brokerOpts, serializer)
		stats := redisstats.NewStatistics(statsOpts)

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

// Register registers a goworker worker function. Class
// refers to the Ruby name of the class which enqueues the
// job. Worker is a function which accepts a queue and an
// arbitrary array of interfaces as arguments.
func Register(class string, worker interfaces.WorkerFunc) {
	globalRegistry.Register(class, worker)
}

// SetSettings sets worker settings
func SetSettings(settings WorkerSettings) {
	workerSettings = settings
}

// Backward compatibility types

// Job represents a job to be processed (backward compatibility)
type Job struct {
	Queue   string
	Payload Payload
}

// Payload represents the job payload (backward compatibility)
type Payload struct {
	Class string        `json:"class"`
	Args  []interface{} `json:"args"`
}

// RedisConn wraps a Redis connection (backward compatibility)
type RedisConn struct {
	redis.Conn
}

func (r *RedisConn) Close() {
	_ = r.Conn.Close()
}

// Enqueue adds a job to the queue
func Enqueue(job *Job) error {
	if err := Init(); err != nil {
		return err
	}

	// Convert old Job type to new interface
	newJob := json_serializer.NewJob(job.Queue, job.Payload.Class, job.Payload.Args)

	return engine.Enqueue(newJob)
}

// GetConn returns a Redis connection (backward compatibility)
func GetConn() (*RedisConn, error) {
	// This is a compatibility shim - the new architecture doesn't expose connections
	return nil, fmt.Errorf("GetConn is deprecated in the new architecture")
}

// PutConn returns a connection to the pool (backward compatibility)
func PutConn(conn *RedisConn) {
	// No-op for compatibility
}

// Namespace returns the namespace
func Namespace() string {
	return workerSettings.Namespace
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

	if !workerSettings.UseNumber {
		logger.Warn("== DEPRECATION WARNING ==")
		logger.Warn("  Currently, encoding/json decodes numbers as float64.")
		logger.Warn("  This can cause numbers to lose precision as they are read from the Resque queue.")
		logger.Warn("  Set the -use-number flag to use json.Number when decoding numbers and remove this warning.")
	}

	return nil
}
