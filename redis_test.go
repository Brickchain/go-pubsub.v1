package pubsub

import (
	"os"
	"testing"
	"time"

	"github.com/Brickchain/go-logger.v1"

	"github.com/satori/go.uuid"
)

func TestNewRedisPubSub(t *testing.T) {
	if os.Getenv("REDIS") == "" {
		t.Skip("No REDIS environment variable set")
	}

	logger.Debug("TestNewRedisPubSub")
	_, _ = NewRedisPubSub(os.Getenv("REDIS"))
}

func TestRedisPubSub_Publish(t *testing.T) {
	if os.Getenv("REDIS") == "" {
		t.Skip("No REDIS environment variable set")
	}

	logger.Debug("TestRedisPubSub_Publish")
	p, err := NewRedisPubSub(os.Getenv("REDIS"))
	if err != nil {
		t.Error(err)
	}

	topic := "/path/to/" + uuid.Must(uuid.NewV4()).String()
	defer p.DeleteTopic(topic)

	err = p.Publish(topic, "doc_id")
	if err != nil {
		t.Error(err)
		return
	}
}

func TestRedisPubSub_Subscribe(t *testing.T) {
	if os.Getenv("REDIS") == "" {
		t.Skip("No REDIS environment variable set")
	}

	logger.Debug("TestRedisPubSub_Subscribe")
	p, err := NewRedisPubSub(os.Getenv("REDIS"))
	if err != nil {
		t.Error(err)
	}

	topic := "/path/to/" + uuid.Must(uuid.NewV4()).String()
	defer p.DeleteTopic(topic)

	sub, err := p.Subscribe("test", topic)
	if err != nil {
		t.Error(err)
	}
	defer sub.Stop(time.Second * 1)

	go func() {
		time.Sleep(time.Millisecond * 300)
		err = p.Publish(topic, "doc_id")
		if err != nil {
			t.Error(err)
			return
		}
	}()

	msg, ok := sub.Pull(time.Second * 3)
	if ok == TIMEOUT {
		t.Error("Pull timed out")
		return
	}

	if msg != "doc_id" {
		t.Errorf("Message returned was not the one we sent. %s != doc_id", msg)
		return
	}

	sub.Stop(time.Second * 10)

	logger.Debug("TestRedisPubSub_Subscribe done")
}
