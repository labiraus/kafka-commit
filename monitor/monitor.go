package main

import (
	"context"
	"fmt"
	"kafka-commit/data"
	"log"
	"os"
	"os/signal"
	"time"
)

const (
	serviceName = "monitor"
	timeout     = time.Second * 10
)

var (
	kafkaURL = "localhost:29092"
	groupID  = ""
)

func main() {
	ctx, ctxDone := context.WithCancel(context.Background())

	done := start(ctx)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	s := <-c
	ctxDone()
	fmt.Println("user got signal: " + s.String() + " now closing")
	<-done
}

func start(ctx context.Context) <-chan struct{} {
	enqueue := make(chan data.SetupContext, 1000)
	dequeue := make(chan string, 1000)
	done := make(chan struct{})

	go func() {
		queue := make(map[string]func())
		for {
			select {
			case <-ctx.Done():
				return

			case key := <-dequeue:
				cancel, exists := queue[key]
				if !exists {
					fmt.Printf("could not find key=%s\n", key)
					continue
				}
				cancel()
				fmt.Println("cancelling: ", key)

			case enqueueRequest := <-enqueue:
				cancel, exists := queue[enqueueRequest.TransactionID]
				if exists {
					cancel()
				}

				childCtx, cancel := context.WithTimeout(ctx, timeout)
				queue[enqueueRequest.TransactionID] = cancel

				go func() {
					<-childCtx.Done()
					delete(queue, enqueueRequest.TransactionID)
					if childCtx.Err() == context.DeadlineExceeded {
						callback(enqueueRequest.CallbackPath)
					} else {
						fmt.Println("cancelled: ", enqueueRequest.TransactionID)
					}
				}()
			}
		}
	}()

	go func() {
		defer close(done)

		control := data.GetKafkaReader(kafkaURL, "control", groupID)
		defer control.Close()
		for {
			m, err := control.ReadMessage(ctx)
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err != nil {
				log.Fatalln(err)
			}
			callbackPath := string(m.Value)
			if len(callbackPath) == 0 {
				fmt.Printf("message at control topic partition:%v offset:%v	%s is started with callback path %v\n", m.Partition, m.Offset, string(m.Key), callbackPath)
				enqueue <- data.SetupContext{TransactionID: string(m.Key), CallbackPath: string(m.Value)}

			} else {
				fmt.Printf("message at control topic partition:%v offset:%v	%s is complete\n", m.Partition, m.Offset, string(m.Key))
				dequeue <- string(m.Key)
			}
		}
	}()

	return done
}

func callback(callbackPath string) {
	fmt.Println("Callback initiated for: ", callbackPath)
}
