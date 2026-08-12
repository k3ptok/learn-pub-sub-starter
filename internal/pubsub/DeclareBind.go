package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func DeclareAndBind(conn *amqp.Connection,	exchange,	queueName,	key string,	queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	newChan, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	newQueue, err := newChan.QueueDeclare(
		queueName, 
		queueType == SimpleQueueDurable, 
		queueType == SimpleQueueTransient, 
		queueType == SimpleQueueTransient, 
		false, 
		amqp.Table{"x-dead-letter-exchange":	"peril_dlx"},
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = newChan.QueueBind(newQueue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return newChan, newQueue, nil
}