package pubsub

import (
	"testing"
	"time"
)

func TestNewInmemPubSub(t *testing.T) {
	_ = NewInmemPubSub()
}

func TestInmemPubSub_Subscribe(t *testing.T) {
	i := NewInmemPubSub()
	_, err := i.Subscribe("group", "topic")
	if err != nil {
		t.Fatal(err)
	}
}

func TestInmemPubSub_Publish(t *testing.T) {
	i := NewInmemPubSub()
	err := i.Publish("topic", "message")
	if err != nil {
		t.Fatal(err)
	}
}

func TestInmemPubSub_DeleteTopic(t *testing.T) {
	i := NewInmemPubSub()
	_, err := i.Subscribe("group", "topic")
	if err != nil {
		t.Fatal(err)
	}

	err = i.DeleteTopic("topic")
	if err != nil {
		t.Fatal(err)
	}
}

func TestInmemSubscriber_Pull(t *testing.T) {
	i := NewInmemPubSub()
	sub, err := i.Subscribe("group", "topic")
	if err != nil {
		t.Fatal(err)
	}

	err = i.Publish("topic", "message")
	if err != nil {
		t.Fatal(err)
	}

	msg, status := sub.Pull(time.Second * 1)
	if status != SUCCESS {
		t.Fatalf("Expected status SUCCESS, got %v", status)
	}

	if msg != "message" {
		t.Fatal("Received message not same as sent message")
	}

}
