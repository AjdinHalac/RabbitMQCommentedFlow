package main

import (
	"bytes"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

/*
 * In the consumer applicaton I will only comment what is not ubiquitous or not clear enough.
 */
func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	/*
	 * The reason we will declare the queue again is because the case of starting the consumer before the producer will leave us with a crashed application if queue does not exist.
	 * Remember that this method is idempotent and won't cause trouble if ran multiple times.
	 * I want to crate a single task queue which is durable and won't lose my message after it stops executing.
	 * If multiple consumers are started, every N-th consumer will get every N-th message.
	 * The issue with this solution is in scenario where all odd message have a huge load while all the even messages are lightly processed.
	 * There still exists an issue where one working dieing will lose the message it was currently processing, also losing all of the messages dispatched to it.
	 * To solve this issue, we will acknowledge messages after they have been completely processed to avoid them getting deleted too early.
	 * Also lets allow our consumers to only read one message at a time, so they get dispatched more evenly.
	 * Take a look at ch.Qos, prefetch count tells the RMQ server to have one message in the consumer till it gets ack-ed.
	 */
	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)

	/*
	 * Analogue to our Publish method in producer we have a Consume method in consumer which reads from the queue.
	 * Consume method accepts few parameters:
	 * - First parameter is a string value holding the name of the queue we are consuming from. In the case below from our freshly declared queue.
	 * - Second is a consumer tag
	 * - Third parameter tells the RabbitMQ that we will manually acknowledge messages when we feel they've been processed.
	 * - Exclusive flag notes that this queue can only be used by the connection that opened it. Exclusive queues are deleted after the connection closes.
	 * - If the no-local flag is set, the RMQ server will not send messages to the connection that published them.
	 * - If the no-wait flag is set the server will not respond to the method.
	 * - Args are additional arguments you find fit for the queue.
	 */
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			dot_count := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dot_count)
			time.Sleep(t * time.Second)
			log.Printf("Done")
			d.Ack(false) // This is worth noting in this block of code. This will ack the messages back to the queue.
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
