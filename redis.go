package pubsub

import (
	"fmt"
	"time"

	"github.com/Brickchain/go-logger.v1"
	"gopkg.in/redis.v5"
)

type RedisPubSub struct {
	client *redis.Client
	addr   string
}

func NewRedisPubSub(addr string) (*RedisPubSub, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	r := RedisPubSub{
		client: client,
		addr:   addr,
	}

	return &r, nil
}

func (r *RedisPubSub) Stop() {
	r.client.Close()
}

func (r *RedisPubSub) Publish(topic string, data string) error {
	res := r.client.Publish(topic, data)
	if res.Err() != nil {
		return res.Err()
	}

	return nil
}

func (r *RedisPubSub) DeleteTopic(topicName string) error {

	return nil
}

type RedisSubscriber struct {
	client  *redis.Client
	topic   string
	sub     *redis.PubSub
	output  chan string
	done    chan bool
	ready   chan bool
	running bool
	err     error
}

func (r *RedisPubSub) Subscribe(group, topic string) (Subscriber, error) {

	client := redis.NewClient(&redis.Options{
		Addr:     r.addr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	s := RedisSubscriber{
		client: client,
		topic:  topic,
		output: make(chan string, 100),
		done:   make(chan bool),
		ready:  make(chan bool),
	}
	go s.run()

	// wait for subscriber to tell us it's ready
	ok := <-s.ready
	if !ok {
		return nil, fmt.Errorf("Something went wrong while setting up the subscriber")
	}

	return &s, nil
}

func (s *RedisSubscriber) run() {
	s.running = true
	defer func() {
		s.running = false
	}()

	var err error
	s.sub, err = s.client.Subscribe(s.topic)
	if err != nil {
		logger.Error("Subscription not created...")
		s.ready <- false
		return
	}

	s.ready <- true
	for {
		select {
		case <-s.done:
			return
		default:
			m, err := s.sub.ReceiveTimeout(time.Millisecond * 100)
			if err != nil {
				continue
			}

			switch msg := m.(type) {
			case *redis.Subscription:
				// Ignore.
			case *redis.Pong:
				// Ignore.
			case *redis.Message:
				s.output <- msg.Payload
			}
		}
	}

}

func (s *RedisSubscriber) Pull(timeout time.Duration) (string, int) {
	select {
	case <-time.After(timeout):
		return "", TIMEOUT
	case m := <-s.output:
		return m, SUCCESS
	}
}

func (s *RedisSubscriber) Chan() chan string {
	return s.output
}

func (s *RedisSubscriber) Stop(timeout time.Duration) {
	end := time.Now().Add(timeout)
	select {
	case <-time.After(timeout):
		return
	case s.done <- true:
		for {
			select {
			case <-time.After(end.Sub(time.Now())):
				return
			default:
				if s.running == false {
					return
				}
			}
		}
	}
}
