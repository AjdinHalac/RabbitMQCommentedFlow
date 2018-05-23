package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("s%: %s", msg, err)
		panic(fmt.Sprintf("s%: %s", msg, err))
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	/*
	 * The core idea behind RMQ is that producer never sends any messages directly to a queue.
	 * The producer can only send messages to an exchange. The exchange must know exactly what to do with recieved messages.
	 * Should it be appended to a specific queue, all of them or none at all. These rules are defined by exchange type.
	 * Exchange types:
	 * - default - Empty string exchange, gets routed based on RoutingKey
	 * - direct - Just like default exchange, the messages are routed based on RoutingKey as long as the queue is binded to the exchange
	 * - topic - Routes messages based on matching between a message routing key and the pattern that was to used to bind a queue to the exchange.
	 * - headers - Ignore the routing key and pass messages based on headers attibute. A message is considered matching if the header arguments equals value on binding
	 * - fanout - Messages are sent to all of the queues and RoutingKey is ignored.
	 */
	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	/*
	 * Since we are publishing to an exchange a routing key is not mandatory because this exchange will ignore it due to the fanout type.
	 */
	body := bodyFrom(os.Args)
	err = ch.Publish(
		"logs", // Exchange
		"",     // Routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}
