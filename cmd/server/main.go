package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os/signal"
	"os"
)

func main() {
	fmt.Println("Starting Peril server...")

	connPath := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connPath)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	fmt.Println("Connection Successful!")

	//wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting Down...")
}
