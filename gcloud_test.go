package pubsub

import (
	"os"
	"strings"
	"testing"

	"github.com/Brickchain/go-logger.v1"

	"github.com/satori/go.uuid"
)

func TestNewGCloudPubSub(t *testing.T) {
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		t.Skip("No PUBSUB_EMULATOR_HOST environment variable set")
	}

	_, _ = NewGCloudPubSub("test", "")
}

func TestGCloudPubSub_Publish(t *testing.T) {
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		t.Skip("No PUBSUB_EMULATOR_HOST environment variable set")
	}

	logger.Debug("TestGCloudPubSub_Publish")
	p, err := NewGCloudPubSub("test", "")
	if err != nil {
		t.Error(err)
	}

	topic := "t" + strings.Replace(uuid.Must(uuid.NewV4()).String(), "-", "", -1)
	defer p.DeleteTopic(topic)

	err = p.Publish(topic, "doc_id")
	if err != nil {
		t.Error(err)
		return
	}
}

func TestGCloudPubSub_Subscribe(t *testing.T) {
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		t.Skip("No PUBSUB_EMULATOR_HOST environment variable set")
	}

	logger.Debug("TestGCloudPubSub_Subscribe")
	p, err := NewGCloudPubSub("test", "")
	if err != nil {
		t.Error(err)
	}

	topic := "t" + strings.Replace(uuid.Must(uuid.NewV4()).String(), "-", "", -1)
	defer p.DeleteTopic(topic)

	sub, err := p.Subscribe("test", topic)
	if err != nil {
		t.Error(err)
	}

	err = p.Publish(topic, "doc_id")
	if err != nil {
		t.Error(err)
		return
	}

	msg, ok := sub.Pull(1)
	if ok == TIMEOUT {
		t.Error("Pull timed out")
		return
	}

	if msg != "doc_id" {
		t.Errorf("Message returned was not the one we sent. %s != doc_id", msg)
		return
	}

	sub.Stop(10)

	logger.Debug("TestGCloudPubSub_Subscribe done")
}
