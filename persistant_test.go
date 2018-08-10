package pubsub

import (
	"os"
	"testing"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
)

func TestNewPersistantPubSub(t *testing.T) {
	db, err := gorm.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	p := NewPersistantPubSub(db)

	if err = p.Publish("test", "test data"); err != nil {
		t.Fatal(err)
	}

	if err = p.Publish("test", "test data 2"); err != nil {
		t.Fatal(err)
	}
}

func TestNewPersistantSubscriber(t *testing.T) {
	defer os.Remove(".test.db")
	db, err := gorm.Open("sqlite3", ".test.db")
	if err != nil {
		t.Fatal(err)
	}
	p := NewPersistantPubSub(db)

	if err = p.Publish("kaka", "kaka data 1"); err != nil {
		t.Fatal(err)
	}

	if err = p.Publish("test", "test data"); err != nil {
		t.Fatal(err)
	}

	if err = p.Publish("test", "test data 2"); err != nil {
		t.Fatal(err)
	}

	s, err := p.Subscribe("test", "test")
	if err != nil {
		t.Fatal(err)
	}

	_, i := s.Pull(1 * time.Second)
	if i != SUCCESS {
		t.Fatalf("Expected SUCCESS (%v), got %v", SUCCESS, i)
	}

	_, i = s.Pull(1 * time.Second)
	if i != SUCCESS {
		t.Fatalf("Expected SUCCESS (%v), got %v", SUCCESS, i)
	}

	if err = p.Publish("kaka", "kaka data 2"); err != nil {
		t.Fatal(err)
	}

	if err = p.Publish("test", "test data 3"); err != nil {
		t.Fatal(err)
	}

	_, i = s.Pull(1 * time.Second)
	if i != SUCCESS {
		t.Fatalf("Expected SUCCESS (%v), got %v", SUCCESS, i)
	}

	s.Stop(2 * time.Second)
}
