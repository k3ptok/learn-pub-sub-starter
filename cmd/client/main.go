package main

import "fmt"

import (
	"time"
	"log"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"os"
	"os/signal"
	"strconv"
)

func main() {
	fmt.Println("Starting Peril client...")

	cmdConn := "amqp://guest:guest@localhost:5672/"

	connCMD, err := amqp.Dial(cmdConn)
	if err != nil {
		log.Fatalf("failed to open connection to RabbitMQ: %v", err)
	}
	defer connCMD.Close()

	channel, err := connCMD.Channel()
	if err != nil {
    	log.Fatalf("failed to open channel: %v", err)
	}
	defer channel.Close()

	fmt.Println("Connection Successful!")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Failed to create username: %s", err)
	}

	//Declare new Gamestate
	newGameState := gamelogic.NewGameState(userName)
	//Handler registers I guess?
	err = pubsub.SubscribeJSON(connCMD, routing.ExchangePerilDirect, "pause" + "." + userName, routing.PauseKey, pubsub.SimpleQueueTransient, handlerPause(newGameState))
	if err != nil {
		log.Fatalf("failed to subjson in client main")
	}
	
	err = pubsub.SubscribeJSON(connCMD, routing.ExchangePerilTopic, "army_moves" + "." + userName, "army_moves.*", pubsub.SimpleQueueTransient, handlerMove(channel, newGameState))
	if err != nil {
		log.Fatalf("failed to subsrcribe to army moves json")
	}

	err = pubsub.SubscribeJSON(connCMD, routing.ExchangePerilTopic, "war", routing.WarRecognitionsPrefix + ".*", pubsub.SimpleQueueDurable, handlerWar(newGameState, channel))
	if err != nil {
		log.Fatalf("failed to war the json")
	}

	// REPL loop
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			fmt.Println("Please enter a command...")
			continue
		}

		switch input[0] {
		case "spawn":
			err = newGameState.CommandSpawn(input)
			if err != nil {
				fmt.Println("failed to spawn unit...")
				continue
			}

		case "move":
			playerMove, err := newGameState.CommandMove(input)
			if err != nil {
				fmt.Printf("%v", err)
				continue
			}

			moveRoutingKey := "army_moves." + userName 
			err = pubsub.PublishJSON(channel, routing.ExchangePerilTopic, moveRoutingKey, playerMove)
			if err != nil {
				log.Println("failed a move command")
				continue
			}

		case "status":
			newGameState.CommandStatus()
		
		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			if len(input) == 1 || len(input) > 2 {
				log.Println("Malformed 'spam' command. Must provide a single integer after the 'spam'command")
				continue
			}

			inputNum := input[1]

			num, err := strconv.Atoi(inputNum)
			if err != nil {
				log.Println("failed to convert str to int on input parse")
				continue
			}

			for i := 0; i < num; i++ {
				malLog := gamelogic.GetMaliciousLog()
				err := publishGameLog(channel, userName, malLog)
				if err != nil {
					log.Println("failed to publish malicious log")
					break
				}
			}

		case "quit":
			gamelogic.PrintQuit()
			os.Exit(1)
		
		default:
			fmt.Println("Invalid command...")
			continue
		}
	}

	//wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting Down...")
}

func publishGameLog(publishCh *amqp.Channel, username, msg string) error {
	return pubsub.PublishGob(
		publishCh,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+username,
		routing.GameLog{
			Username:    username,
			CurrentTime: time.Now(),
			Message:     msg,
		},
	)
}
