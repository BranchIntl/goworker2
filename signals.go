package goworker

import (
	"os"
	"os/signal"
	"syscall"
)

// signals returns a channel that receives a signal when the process should quit
func signals() <-chan bool {
	quit := make(chan bool)

	go func() {
		signals := make(chan os.Signal)
		defer close(signals)

		signal.Notify(signals, syscall.SIGQUIT, syscall.SIGTERM, os.Interrupt)
		defer signal.Stop(signals)

		<-signals
		quit <- true
	}()

	return quit
}
