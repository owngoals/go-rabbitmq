package gorabbitmq

import (
	"github.com/streadway/amqp"
	"testing"
	"time"
)

const (
	testUser     = "root"
	testPassword = "root"
	testHost     = "127.0.0.1"
	testPort     = 5672
	testQueue    = "go.rabbitmq"
)

func TestEnqueue(t *testing.T) {
	conn, err := Conn(testUser, testPassword, testHost, testPort)
	if err != nil {
		t.FailNow()
	}
	defer conn.Close()

	go CreateQueue(conn, testQueue, func(d amqp.Delivery) error {
		t.Logf("body: %s\n", string(d.Body))
		return nil
	})
	time.Sleep(5 * time.Second)

	body := "Hello rabbitmq"
	if err := Enqueue(conn, testQueue, body); err != nil {
		t.FailNow()
	}

	time.Sleep(5 * time.Second)
}
