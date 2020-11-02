package rpcp

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

const handshake = "1lann/rpcp: handshake"

var errType = reflect.TypeOf((*error)(nil)).Elem()

// Packet represents a packet that is sent over the wire.
type Packet struct {
	Event       string             `msgpack:"v"`
	ID          string             `msgpack:"i"`
	Payload     msgpack.RawMessage `msgpack:"p"`
	IsError     bool               `msgpack:"e"`
	ExpectReply bool               `msgpack:"r"`
}

// Handler represents the handler used for receiving events.
type Handler interface{}

// Client represents a client.
type Client struct {
	conn    net.Conn
	encoder *msgpack.Encoder
	decoder *msgpack.Decoder
	writer  *gzip.Writer

	sendMutex    *sync.Mutex
	receiptMutex *sync.Mutex

	handlers map[string]Handler
	receipts map[string]chan<- Packet
}

// NewClient returns a new client on the given connection.
func NewClient(conn net.Conn) (*Client, error) {
	if tcpconn, ok := conn.(*net.TCPConn); ok {
		tcpconn.SetKeepAlive(true)
		tcpconn.SetKeepAlivePeriod(5 * time.Second)
	}

	conn.SetDeadline(time.Now().Add(5 * time.Second))

	wr := gzip.NewWriter(conn)

	done := make(chan struct{})
	go func() {
		wr.Write([]byte(handshake))
		wr.Flush()
		close(done)
	}()

	rd, err := gzip.NewReader(conn)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, len(handshake))
	if _, err := io.ReadFull(rd, buffer); err != nil {
		return nil, err
	}

	if !bytes.Equal(buffer, []byte(handshake)) {
		return nil, errors.New("rpcp: invalid handshake")
	}

	<-done

	conn.SetDeadline(time.Time{})

	return &Client{
		conn:    conn,
		encoder: msgpack.NewEncoder(wr),
		decoder: msgpack.NewDecoder(rd),
		writer:  wr,

		sendMutex:    new(sync.Mutex),
		receiptMutex: new(sync.Mutex),

		handlers: make(map[string]Handler),
		receipts: make(map[string]chan<- Packet),
	}, nil
}

// On registers a handler for handling an event.
func (c *Client) On(event string, handler Handler) {
	t := reflect.TypeOf(handler)
	if t.Kind() != reflect.Func || t.NumIn() != 1 || t.NumOut() != 2 ||
		!t.Out(1).Implements(errType) {
		panic(fmt.Sprintf("rpcp: event handler %q must be a function with signature func(T) (R, error)", event))
	}

	c.handlers[event] = handler
}

func (c *Client) handleError(p Packet, err error) {
	if p.ExpectReply {
		go c.fire("__reply__", p.ID, false, true, err.Error())
	} else {
		log.Printf("rpcp: unhandled handler error: %v", err)
	}
}

// Receive runs the receive loop to process incoming packets. It returns
// when the connection dies, and a new client will need to be spawned.
func (c *Client) Receive() error {
	for {
		var p Packet
		if err := c.decoder.Decode(&p); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}

			return err
		}

		c.receiptMutex.Lock()
		receipt, found := c.receipts[p.ID]
		c.receiptMutex.Unlock()
		if found {
			receipt <- p
			continue
		}

		if p.Event == "__reply__" {
			continue
		}

		if handler, found := c.handlers[p.Event]; found {
			elem := reflect.New(reflect.TypeOf(handler).In(0)).Interface()
			err := msgpack.Unmarshal(p.Payload, elem)
			if err != nil {
				c.handleError(p, err)
				continue
			}

			out := reflect.ValueOf(handler).Call([]reflect.Value{
				reflect.ValueOf(elem).Elem(),
			})

			if !out[1].IsNil() {
				err = out[1].Interface().(error)
				c.handleError(p, err)
				continue
			}

			if p.ExpectReply {
				go func() {
					err := c.fire("__reply__", p.ID, false, false, out[0].Interface())
					if err != nil {
						c.handleError(p, err)
					}
				}()
			}
		} else {
			log.Println("rpcp: unknown handler for event:", p.Event)
			continue
		}
	}
}

func (c *Client) fire(event, id string, expectReply bool, isError bool, payload interface{}) error {
	c.sendMutex.Lock()
	defer c.sendMutex.Unlock()

	payloadData, err := msgpack.Marshal(payload)
	if err != nil {
		return err
	}

	if err := c.encoder.Encode(Packet{
		Event:       event,
		ID:          id,
		ExpectReply: expectReply,
		IsError:     isError,
		Payload:     payloadData,
	}); err != nil {
		return err
	}

	return c.writer.Flush()
}

// Fire fires an event with the given payload asynchronously.
func (c *Client) Fire(event string, payload interface{}) error {
	return c.fire(event, uuid.New().String(), false, false, payload)
}

// Do performs a blocking request that blocks until the handler on the
// remote end responds. The value of the response is returned.
func (c *Client) Do(event string, payload interface{}, result interface{}) error {
	id := uuid.New().String()
	receive := make(chan Packet, 1)

	c.receiptMutex.Lock()
	c.receipts[id] = receive
	c.receiptMutex.Unlock()

	defer func() {
		c.receiptMutex.Lock()
		delete(c.receipts, id)
		c.receiptMutex.Unlock()
	}()

	if err := c.fire(event, id, true, false, payload); err != nil {
		return err
	}

	packetResult := <-receive
	if packetResult.IsError {
		var errMsg string
		err := msgpack.Unmarshal(packetResult.Payload, &errMsg)
		if err != nil {
			return fmt.Errorf("rpcp: failed to unmarshal error string: %w", err)
		}

		return errors.New(errMsg)
	}

	err := msgpack.Unmarshal(packetResult.Payload, result)
	if err != nil {
		return fmt.Errorf("rpcp: failed to unmarshal: %w", err)
	}

	return nil
}

// Close closes the underlying client connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
