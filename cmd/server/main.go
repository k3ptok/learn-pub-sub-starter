package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os/signal"
	"os"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
)

func main() {
	fmt.Println("Starting Peril server...")

	serverConn := "amqp://guest:guest@localhost:5672/"

	connServer, err := amqp.Dial(serverConn)
	if err != nil {
		log.Fatalf("failed to open connection to RabbitMQ: %v", err)
	}
	defer connServer.Close()

	fmt.Println("Connection Successful!")

	rabbitChanServer, err := connServer.Channel()
	if err != nil {
		log.Fatalf("Failed to create Channel for RabbitMQ connection: %v", err)
	}
	defer rabbitChanServer.Close()

	err = pubsub.SubscribeGob(connServer, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug + ".*", pubsub.SimpleQueueDurable, func(log routing.GameLog) pubsub.Acktype{
		defer fmt.Print("> ")
    	err := gamelogic.WriteLog(log)
    	if err != nil {
        	return pubsub.NackRequeue
    	}
    	return pubsub.Ack
	})
	if err != nil {
		log.Fatalf("failed to generate bound channel and queue: %s", err)
	}

	err = pubsub.PublishJSON(rabbitChanServer, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused:	true,
	})
	if err != nil {
		log.Fatalf("Failed to publish JSON through RabbitMQ channel: %v", err)
	}

	

	gamelogic.PrintServerHelp()

	for {
		//Grab player input
		input := gamelogic.GetInput()
		if len(input) == 0 {
			fmt.Println("Please enter a command...")
			continue
		}
		//Process player pause input
		if input[0] == "pause" {
			log.Println("Pausing...")
			err = pubsub.PublishJSON(rabbitChanServer, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
			IsPaused:	true,
			})
			if err != nil {
				log.Printf("Failed to pause: %v", err)
			} 
		} else if input[0] == "resume" {
			log.Println("Resuming...")
			err = pubsub.PublishJSON(rabbitChanServer, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused:	false,
			})
			if err != nil {
				log.Printf("Failed to resume: %v", err)
			}
		} else if input[0] == "quit" {
			log.Println("Quitting...")
			break
		} else {
			log.Println("Invalid command...")
		}
	}

	//wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting Down...")
}
