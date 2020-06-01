package gorabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"strconv"
)

// Consumer 队列消费者
type Consumer func(amqp.Delivery) error

// Conn 连接 rabbitmq
func Conn(user, password, host string, port int) (*amqp.Connection, error) {
	return amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/", user, password, host, port))
}

// Enqueue 放入队列
func Enqueue(conn *amqp.Connection, queue, body string) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare(
		queue,
		amqp.ExchangeTopic,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	return ch.Publish(
		queue,
		queue+".*",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
}

// CreateQueue 创建一个队列
func CreateQueue(conn *amqp.Connection, queue string, consumers ...Consumer) {
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	exchange := "exchange." + queue
	if err := ch.ExchangeDeclare(
		exchange,
		amqp.ExchangeTopic,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		panic(err)
	}

	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	if err := ch.QueueBind(
		q.Name,
		queue+".*",
		exchange,
		false,
		nil,
	); err != nil {
		panic(err)
	}

	msg, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	forever := make(chan bool)
	go func() {
		for d := range msg {
			for _, c := range consumers {
				if err := c(d); err != nil {
					log.Fatalf("rabbmitmq consumer error: %v", err)
				}
			}
		}
	}()
	<-forever
}

// EnqueueDelay 放入延迟队列
func EnqueueDelay(conn *amqp.Connection, queue, body string, ttl int) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ch.Publish(
		"",
		queue+".delay",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
			Expiration:  strconv.Itoa(ttl * 1000),
		},
	)
}

// CreateDelayQueue 创建一个延迟队列
func CreateDelayQueue(conn *amqp.Connection, queue string, consumers ...Consumer) {
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	exchange := "exchange." + queue
	if err := ch.ExchangeDeclare(
		exchange,
		amqp.ExchangeFanout,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		panic(err)
	}

	q, err := ch.QueueDeclare(
		queue,
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	if _, err := ch.QueueDeclare(
		queue+".delay",
		false,
		false,
		true,
		false,
		amqp.Table{"x-dead-letter-exchange": exchange},
	); err != nil {
		panic(err)
	}

	if err := ch.QueueBind(
		q.Name,
		"",
		exchange,
		false,
		nil,
	); err != nil {
		panic(err)
	}

	msg, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	forever := make(chan bool)
	go func() {
		for d := range msg {
			for _, c := range consumers {
				if err := c(d); err != nil {
					log.Fatalf("rabbmitmq consumer error: %v", err)
				}
			}
		}
	}()
	<-forever
}
