package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/benmanns/goworker"
)

func main() {
	// Parse flags
	flag.Parse()

	// Check if queues are specified
	if len(flag.Args()) == 0 {
		fmt.Println("goworker: a Go-based background worker")
		fmt.Println("\nUsage: goworker [options]")
		fmt.Println("\nOptions:")
		flag.PrintDefaults()
		fmt.Println("\nExample:")
		fmt.Println("  goworker -queues=myqueue -concurrency=25")
		os.Exit(1)
	}

	// Start worker
	if err := goworker.Work(); err != nil {
		log.Fatal("Error:", err)
	}
}
