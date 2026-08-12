package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"encoding/json"
	"log"
	"fmt"
	"encoding/gob"
	"bytes"
)

type Acktype int

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T)Acktype) error {
	subChan, subQueue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	err = subChan.Qos(10, 0, false)
	if err != nil {
		return err
	}

	deliveryChan, err := subChan.Consume(subQueue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func(){
		

		for msg := range deliveryChan {
			var raw T
			err := json.Unmarshal(msg.Body, &raw)
			if err != nil {
				log.Println("Failed to unmarshal from json")
				continue
			}
			switch handler(raw) {
			case Ack:
				msg.Ack(false)
				fmt.Println("Ack")
			case NackDiscard:
				msg.Nack(false, false)
				fmt.Println("NackDiscard")
			case NackRequeue:
				msg.Nack(false, true)
				fmt.Println("NackRequeue")
			}
		}
	}()
	return nil
}

func SubscribeGob[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T)Acktype) error{
	subChan, subQueue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	err = subChan.Qos(10, 0, false)
	if err != nil {
		return err
	}

	deliveryChan, err := subChan.Consume(subQueue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func(){
		for msg := range deliveryChan {
			var raw T
			buffer := bytes.NewBuffer(msg.Body)
			decoder := gob.NewDecoder(buffer)
			err := decoder.Decode(&raw)
			if err != nil {
				log.Println("Failed to decode gob")
				continue
			}

			switch handler(raw) {
			case Ack:
				msg.Ack(false)
				fmt.Println("Ack")
			case NackDiscard:
				msg.Nack(false, false)
				fmt.Println("NackDiscard")
			case NackRequeue:
				msg.Nack(false, true)
				fmt.Println("NackRequeue")
			}
	}
	}()
	return nil
}