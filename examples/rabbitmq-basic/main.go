package main

import (
	"context"
	"fmt"
	"log"

	"github.com/BranchIntl/goworker2/core"
	"github.com/BranchIntl/goworker2/engines"
)

func myFunc(queue string, args ...interface{}) error {
	fmt.Printf("From %s, %v\n", queue, args)
	return nil
}

func main() {
	// Create a pre-configured Sneakers engine (RabbitMQ + Sneakers serializer + NoOp stats)
	options := engines.DefaultSneakersOptions()
	options.RabbitMQURI = "amqp://guest:guest@localhost:5672/"
	options.Queues = []string{"myqueue"}
	options.EngineOptions = []core.EngineOption{
		core.WithConcurrency(2),
	}

	engine := engines.NewSneakersEngine(options)

	// Register job handler
	engine.Register("MyClass", myFunc)

	// Start the engine and wait for shutdown signals
	ctx := context.Background()
	if err := engine.Run(ctx); err != nil {
		log.Fatal("Error:", err)
	}

	// Alternative: Use MustRun for simpler error handling (panics on error)
	// engine.MustRun(ctx)

	// For manual control, you can still use the underlying engine:
	// if err := engine.Start(ctx); err != nil {
	//     log.Fatal("Error:", err)
	// }
	//
	// // Set up signal handling manually
	// sigChan := make(chan os.Signal, 1)
	// signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	//
	// // Wait for shutdown signal
	// sig := <-sigChan
	// fmt.Printf("Received signal %v, shutting down...\n", sig)
	//
	// // Stop the engine
	// if err := engine.Stop(); err != nil {
	//     log.Printf("Error stopping engine: %v", err)
	// }
}
