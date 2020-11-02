package rpc

import (
	"encoding/gob"
	"errors"
	"net"
	"testing"
	"time"
)

func TestFire(t *testing.T) {
	pa, pb := net.Pipe()
	var a, b *Client

	done := make(chan struct{})
	go func() {
		var err error
		a, err = NewClient(pa)
		if err != nil {
			t.Fatal(err)
		}
		close(done)
	}()

	var err error
	b, err = NewClient(pb)
	if err != nil {
		t.Fatal(err)
	}

	<-done

	success := make(chan bool, 2)
	b.On("test", func(str string) (struct{}, error) {
		if str == "Hello, world!" {
			success <- true
		} else {
			t.Fatal("payload is not hello world")
			success <- false
		}

		return struct{}{}, nil
	})

	go b.Receive()

	a.Fire("test", "Hello, world!")

	go func() {
		<-time.After(time.Second)
		success <- false
	}()

	if !<-success {
		t.Fatal("test unsuccessful")
	}
}

type dataStruct struct {
	Name string
	Age  int
}

func TestDoResponse(t *testing.T) {
	gob.Register(dataStruct{})

	pa, pb := net.Pipe()
	var a, b *Client

	done := make(chan struct{})
	go func() {
		var err error
		a, err = NewClient(pa)
		if err != nil {
			t.Fatal(err)
		}
		close(done)
	}()

	var err error
	b, err = NewClient(pb)
	if err != nil {
		t.Fatal(err)
	}

	<-done

	b.On("special", func(p *dataStruct) (*dataStruct, error) {
		if p.Name != "Initial" || p.Age != 10 {
			t.Fatal("payload does not match expected value")
		}

		return &dataStruct{
			Name: "Responder",
			Age:  1,
		}, nil
	})

	b.On("fail", func(_ struct{}) (struct{}, error) {
		return struct{}{}, errors.New("rpcp_test: test error")
	})

	go b.Receive()
	go a.Receive()

	var res dataStruct
	err = a.Do("special", &dataStruct{
		Name: "Initial",
		Age:  10,
	}, &res)
	if err != nil {
		t.Fatal(err)
	}

	if res.Name != "Responder" || res.Age != 1 {
		t.Fatal("payload does not match expected value")
	}

	err = a.Do("fail", struct{}{}, &(struct{}{}))
	if err == nil {
		t.Fatal("error should not be nil")
	}

	if err.Error() != "rpcp_test: test error" {
		t.Fatal("error failed to be populated with correct value:", err)
	}
}
