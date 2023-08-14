package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"kafka-commit/data"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

const (
	serviceName = "forwarder"
)

var (
	kafkaURL = "localhost:29092"
)

func main() {
	flag.Parse()
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
	done := make(chan struct{})

	go func() {
		defer close(done)
		srv := &http.Server{Addr: ":8080"}
		go func() {
			<-ctx.Done()
			err := srv.Shutdown(ctx)
			if err != nil {
				panic(err)
			}
		}()
		http.HandleFunc("/setup", setupHandler)
		http.HandleFunc("/cancel", cancelHandler)
		http.HandleFunc("/commit/", commitHandler)
		http.HandleFunc("/event/", eventHandler)

		fmt.Println("starting")
		if err := http.ListenAndServe(":8080", nil); err != http.ErrServerClosed {
			fmt.Println(fmt.Errorf("HTTP server shut down unexpectedly: %s", err))
		}
	}()

	return done
}

func log(function string, context data.RequestContext, err error) {
	fmt.Printf("serviceName=%s function=%s contextID=%s error='%s'\n", serviceName, function, context.ContextID, err)
}

func getContext(r *http.Request) (data.RequestContext, error) {
	context := data.RequestContext{}
	contextID := r.Header.Get("ContextID")
	if contextID == "" {
		return context, fmt.Errorf("ContextID header not provided")
	}
	context.ContextID = contextID

	// Add further context headers here
	return context, nil
}

func newKafkaWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func kafkaWrite(topic string, key string, body []byte) error {
	fmt.Printf("topic: %s, key: %s, body length: %v\n", topic, key, len(body))
	fmt.Println(string(body))
	msg := kafka.Message{
		Key:   []byte(key),
		Value: body,
	}
	writer := newKafkaWriter(topic)
	defer writer.Close()
	err := writer.WriteMessages(context.Background(), msg)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("produced", key)
	}
	return nil
}

func setupHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var context data.RequestContext
	defer func() {
		if err != nil {
			log("setup", context, err)
		}
	}()

	if r.Method != http.MethodPost {
		err = fmt.Errorf("method not allowed: %v", r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	context, err = getContext(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}

	callbackPath := r.Header.Get("CallbackPath")
	if callbackPath == "" {
		err = fmt.Errorf("CallbackPath header not provided")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	transactionID := uuid.New().String()

	err = kafkaWrite("control", transactionID, []byte(callbackPath))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	out, err := json.Marshal(data.SetupResponse{
		TransactionID: transactionID,
	})

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ContentLength", strconv.Itoa(len(out)))
	_, err = w.Write(out)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		err = fmt.Errorf("could not write data: %v", err)
	}
}

func cancelHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var context data.RequestContext
	defer func() {
		if err != nil {
			log("cancel", context, err)
		}
	}()

	if r.Method != http.MethodPost {
		err = fmt.Errorf("method not allowed: %v", r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	context, err = getContext(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}

	transactionID := r.Header.Get("TransactionID")
	if transactionID == "" {
		err = fmt.Errorf("TransactionID header not provided")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = kafkaWrite("control", transactionID, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func commitHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var context data.RequestContext
	defer func() {
		if err != nil {
			log("commit", context, err)
		}
	}()

	if r.Method != http.MethodPost {
		err = fmt.Errorf("method not allowed: %v", r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	context, err = getContext(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}

	transactionID := r.Header.Get("TransactionID")
	if transactionID == "" {
		err = fmt.Errorf("TransactionID header not provided")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	key := r.Header.Get("Key")
	if key == "" {
		err = fmt.Errorf("Key header not provided")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	path := strings.ReplaceAll(r.URL.Path, "/", ".")
	if path == ".commit." {
		err = fmt.Errorf("Path incomplete")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	path = path[len(".commit."):]

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = kafkaWrite("data."+path, key, body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = kafkaWrite("control", transactionID, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func eventHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var context data.RequestContext
	defer func() {
		if err != nil {
			log("event", context, err)
		}
	}()

	if r.Method != http.MethodPost {
		err = fmt.Errorf("method not allowed: %v", r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	context, err = getContext(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
	}

	key := r.Header.Get("Key")
	if key == "" {
		err = fmt.Errorf("Key header not provided")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	path := strings.ReplaceAll(r.URL.Path, "/", ".")
	if path == "event." {
		err = fmt.Errorf("path incomplete")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	path = path[len(".event."):]
	var body []byte
	_, err = r.Body.Read(body)

	err = kafkaWrite("event."+path, key, body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
