// Package goworker is an ActiveJob-compatible, Go-based
// background worker. It allows you to push jobs into a
// queue using an expressive language like Ruby while
// harnessing the efficiency and concurrency of Go to
// minimize job latency and cost.
//
// goworker supports multiple queue backends like:
// - Redis (Resque)
// - RabbitMQ (Kicks)
// - Bring Your Own
//
// goworker supports multiple statistics systems like:
// - Resque
// - Bring Your Own
//
// # Example
//
//	package main
//
//	import (
//		"context"
//		"github.com/benmanns/goworker/core"
//		"github.com/benmanns/goworker/brokers/redis"
//		resqueStats "github.com/benmanns/goworker/statistics/resque"
//		"github.com/benmanns/goworker/registry"
//		"github.com/benmanns/goworker/serializers/json"
//	)
//
//	func main() {
//		// Create components
//		broker := redis.NewBroker(redis.DefaultOptions(), json.NewSerializer())
//		stats := resqueStats.NewStatistics(resqueStats.DefaultOptions())
//		reg := registry.NewRegistry()
//
//		// Create engine
//		engine := core.NewEngine(
//			broker,
//			stats,
//			reg,
//			json.NewSerializer(),
//			core.WithConcurrency(50),
//			core.WithQueues([]string{"critical", "default"}),
//		)
//
//		// Register workers
//		reg.Register("EmailJob", sendEmail)
//
//		// Start processing and wait for shutdown signals
//		ctx := context.Background()
//		if err := engine.Run(ctx); err != nil {
//			panic(err)
//		}
//	}
//
// # Sharing Resources
//
// To create workers that share a database pool or other
// resources, use a closure to share variables.
//
//	package main
//
//	import (
//		"fmt"
//		"github.com/benmanns/goworker"
//	)
//
//	func newMyFunc(uri string) (func(queue string, args ...interface{}) error) {
//		foo := NewFoo(uri)
//		return func(queue string, args ...interface{}) error {
//			foo.Bar(args)
//			return nil
//		}
//	}
//
//	func init() {
//		goworker.Register("MyClass", newMyFunc("http://www.example.com/"))
//	}
//
//	func main() {
//		if err := goworker.Work(); err != nil {
//			fmt.Println("Error:", err)
//		}
//	}
//
// # Type Assertions and Parameters
//
// goworker worker functions receive the queue they are
// serving and a slice of interfaces. To use them as
// parameters to other functions, use Go type assertions
// to convert them into usable types.
//
//	// Expecting (int, string, float64)
//	func myFunc(queue string, args ...interface{}) error {
//		idNum, ok := args[0].(json.Number)
//		if !ok {
//			return errorInvalidParam
//		}
//		id, err := idNum.Int64()
//		if err != nil {
//			return errorInvalidParam
//		}
//		name, ok := args[1].(string)
//		if !ok {
//			return errorInvalidParam
//		}
//		weightNum, ok := args[2].(json.Number)
//		if !ok {
//			return errorInvalidParam
//		}
//		weight, err := weightNum.Float64()
//		if err != nil {
//			return errorInvalidParam
//		}
//		doSomething(id, name, weight)
//		return nil
//	}
//
// # Testing with Mock Brokers
//
// goworker includes mock implementations for easy testing
// without external dependencies:
//
//	import "github.com/benmanns/goworker/testing/mocks"
//
//	func TestWorker(t *testing.T) {
//		broker := mocks.NewMockBroker()
//		stats := mocks.NewMockStatistics()
//		engine := core.NewEngine(broker, stats, reg, serializer)
//
//		// Add test job
//		job := mocks.NewMockJob("myqueue", "MyClass", "arg1", "arg2")
//		broker.Enqueue(context.Background(), job)
//
//		// Process jobs
//		engine.Start(context.Background())
//	}
//
// For integration testing with Redis, use redis-cli to
// insert jobs onto the Redis queue:
//
//	redis-cli -r 100 RPUSH resque:queue:myqueue '{"class":"MyClass","args":["hi","there"]}'
//
// will insert 100 jobs for the MyClass worker onto the
// myqueue queue. It is equivalent to:
//
//	class MyClass
//	  @queue = :myqueue
//	end
//
//	100.times do
//	  Resque.enqueue MyClass, ['hi', 'there']
//	end
//
// # Configuration
//
//	engine := core.NewEngine(
//		broker,
//		stats,
//		registry,
//		serializer,
//		core.WithConcurrency(25),
//		core.WithQueues([]string{"high", "medium", "low"}),
//		core.WithPollInterval(5 * time.Second),
//		core.WithExitOnComplete(true),
//	)
package goworker
