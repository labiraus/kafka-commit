package main

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka-commit/data"
	"log"
	"os"
	"os/signal"
	"time"
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
	done := make(chan struct{})

	go func() {
		defer close(done)
		validators := getValidators()

		doneChans := make([]<-chan struct{}, 0, len(validators))

		for _, validator := range validators {
			doneChans = append(doneChans, startValidator(validator, ctx))
		}
		for _, doneChan := range doneChans {
			<-doneChan
		}
	}()

	return done
}

func startValidator(validator data.Validator, ctx context.Context) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)

		topic := data.GetKafkaReader(kafkaURL, validator.Topic, groupID)
		defer topic.Close()
		for {
			kafkaMessage, err := topic.ReadMessage(ctx)
			if err != nil {
				log.Fatalln(err)
			}
			select {
			case <-ctx.Done():
				return
			default:
			}

			var message map[string]interface{}
			err = json.Unmarshal(kafkaMessage.Value, &message)
			if err != nil || !checkMessage(message, validator.Properties, ctx) {
				fmt.Println("*************")
				if err != nil {
					fmt.Println("error:", err)
				}

				switch validator.Consequence {

				case data.ConsequenceEmail:
					fmt.Println("email")
					fmt.Println(validator.Endpoint)
					fmt.Println(validator.Template)
					fmt.Println(string(kafkaMessage.Value))

				case data.ConsequenceApi:
					fmt.Println("api")
					fmt.Println(validator.Endpoint)
					fmt.Println(string(kafkaMessage.Value))
				}
			} else {
				fmt.Println("all clean, topic: ", validator.Topic, " key: ", string(kafkaMessage.Key))
			}

		}
	}()
	return done
}

func checkMessage(message map[string]interface{}, properties []data.Property, ctx context.Context) bool {
	for _, property := range properties {
		select {
		case <-ctx.Done():
			return true
		default:
		}

		value, exists := message[property.Name]
		if !exists && property.Optional == data.ValidOptional {
			continue
		}

		if !exists {
			fmt.Println("could not find property: ", property.Name)
			fmt.Printf("%+v\n", message)
			return false
		}

		switch property.Type {
		case data.PropStruct:
			node, ok := value.(map[string]interface{})
			if !ok {
				fmt.Println("property not a struct: ", property.Name)
				return false
			}
			if len(property.Children) > 0 && !checkMessage(node, property.Children, ctx) {
				return false
			}

		case data.PropBool:
			_, ok := value.(bool)
			if !ok {
				fmt.Println("property not a bool: ", property.Name)
				return false
			}

		case data.PropInt:
			val, ok := value.(int)
			if !ok {
				fmt.Println("property not an int: ", property.Name)
				return false
			}
			if property.Optional == data.ValidGreaterThan0 && val <= 0 {
				fmt.Println("int property not greater than 0: ", property.Name)
				return false
			}

		case data.PropInt64:
			val, ok := value.(int64)
			if !ok {
				fmt.Println("property not an int64: ", property.Name)
				return false
			}
			if property.Optional == data.ValidGreaterThan0 && val <= 0 {
				fmt.Println("int64 property not greater than 0: ", property.Name)
				return false
			}

		case data.PropFloat32:
			val, ok := value.(float32)
			if !ok {
				fmt.Println("property not a float32: ", property.Name)
				return false
			}
			if property.Optional == data.ValidGreaterThan0 && val <= 0 {
				fmt.Println("float32 property not greater than 0: ", property.Name)
				return false
			}

		case data.PropFloat64:
			val, ok := value.(float64)
			if !ok {
				fmt.Println("property not a float64: ", property.Name)
				return false
			}
			if property.Optional == data.ValidGreaterThan0 && val <= 0 {
				fmt.Println("float64 property not greater than 0: ", property.Name)
				return false
			}

		case data.PropString:
			val, ok := value.(string)
			if !ok {
				fmt.Println("property not a string: ", property.Name)
				return false
			}
			if property.Optional == data.ValidNotBlank && len(val) == 0 {
				fmt.Println("string property was blank: ", property.Name)
				return false
			}

		case data.PropTime:
			val, ok := value.(time.Time)
			if !ok {
				fmt.Println("property not a time: ", property.Name)
				return false
			}
			if property.Optional == data.ValidGreaterThan0 && val.IsZero() {
				fmt.Println("time property not greater than 0: ", property.Name)
				return false
			}

		}
	}

	return true
}

func getValidators() []data.Validator {
	validators := make([]data.Validator, 0, 1)
	validators = append(validators, data.Validator{
		Topic:       "data.test",
		Consequence: data.ConsequenceEmail,
		Endpoint:    "alert@email.com",
		Template:    "Something went wrong with the message:",
		Properties: []data.Property{
			{
				Name:     "example",
				Type:     data.PropBool,
				Optional: data.ValidMandatory,
			},
		},
	})
	return validators
}
