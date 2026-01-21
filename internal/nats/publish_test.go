package nats

import (
	"testing"

	natslib "github.com/nats-io/nats.go"
)

type fakePublisher struct {
	lastSubject string
	lastData    []byte
	err         error
}

func (f *fakePublisher) Publish(subject string, data []byte, opts ...natslib.PubOpt) (*natslib.PubAck, error) {
	f.lastSubject = subject
	f.lastData = append([]byte(nil), data...)
	return &natslib.PubAck{}, f.err
}

func TestPublish_ValidatesInputs(t *testing.T) {
	fp := &fakePublisher{}
	if _, err := Publish(nil, fp, "", []byte("x")); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := Publish(nil, fp, "s", nil); err == nil {
		t.Fatalf("expected error")
	}
}

func TestPublish_Publishes(t *testing.T) {
	fp := &fakePublisher{}
	_, err := Publish(nil, fp, "a", []byte("b"))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if fp.lastSubject != "a" {
		t.Fatalf("subject: %q", fp.lastSubject)
	}
	if string(fp.lastData) != "b" {
		t.Fatalf("data: %q", string(fp.lastData))
	}
}

func TestPublishDLQ_UsesDLQSubject(t *testing.T) {
	fp := &fakePublisher{}
	_, err := PublishDLQ(nil, fp, "x.y", []byte("z"))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if fp.lastSubject != "x.y.dlq" {
		t.Fatalf("subject: %q", fp.lastSubject)
	}
}
