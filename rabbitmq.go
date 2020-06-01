package gorabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
)

func Conn(user, password, host string, port int) (*amqp.Connection, error) {
	return amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/", user, password, host, port))
}
