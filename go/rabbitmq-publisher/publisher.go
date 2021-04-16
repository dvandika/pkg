package rabbitmq_publisher

import (
	"encoding/json"
	"log"
	"time"

	"github.com/streadway/amqp"
)

type Publisher struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewPublisher(amqpUrl string) (*Publisher, error) {
	conn, err := amqp.Dial(amqpUrl)

	if err != nil {
		log.Fatalf("%s: %s", "Failed to connect to RabbitMQ", err)
		return nil, err
	}

	ch, err := conn.Channel()

	if err != nil {
		log.Fatalf("%s: %s", "Failed to connect to RabbitMQ", err)
		return nil, err
	}

	return &Publisher{
		ch:   ch,
		conn: conn,
	}, nil
}

func (p *Publisher) PublishInstruction(qd string, data interface{}) error {
	// Declare Queue

	q, err := p.ch.QueueDeclare(
		qd,    // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		log.Fatalf("%s: %s", "Failed to declare a queue", err)
		return err
	}

	// Encode Data
	body, err := json.Marshal(data)

	if err != nil {
		log.Fatalf("%s: %s", "Failed to marshaling JSON", err)
		return err
	}

	// Publishing a message

	err = p.ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			ContentType:  "application/json",
			Body:         []byte(body),
		})

	if err != nil {
		log.Fatalf("%s: %s", "Failed to publish a message", err)
		return err
	}

	defer p.conn.Close()

	defer p.ch.Close()

	return err
}
