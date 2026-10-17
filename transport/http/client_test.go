package http

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"sync"
	"testing"
	"time"

	smithy "github.com/aws/smithy-go"
)

func TestClientHandler_Handle(t *testing.T) {
	cases := map[string]struct {
		Context   context.Context
		Client    ClientDo
		ExpectErr func(error) error
	}{
		"no error": {
			Context: context.Background(),
			Client: ClientDoFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{}, nil
			}),
		},
		"send error": {
			Context: context.Background(),
			Client: ClientDoFunc(func(*http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("some error")
			}),
			ExpectErr: func(err error) error {
				var sendError *RequestSendError
				if !errors.As(err, &sendError) {
					return fmt.Errorf("expect error to be %T, %v", sendError, err)
				}

				var cancelError *smithy.CanceledError
				if errors.As(err, &cancelError) {
					return fmt.Errorf("expect error to not be %T, %v", cancelError, err)
				}

				return nil
			},
		},
		"canceled error": {
			Context: func() context.Context {
				ctx, fn := context.WithCancel(context.Background())
				fn()
				return ctx
			}(),
			Client: ClientDoFunc(func(*http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("some error")
			}),
			ExpectErr: func(err error) error {
				var sendError *RequestSendError
				if errors.As(err, &sendError) {
					return fmt.Errorf("expect error to not be %T, %v", sendError, err)
				}

				var cancelError *smithy.CanceledError
				if !errors.As(err, &cancelError) {
					return fmt.Errorf("expect error to be %T, %v", cancelError, err)
				}

				return nil
			},
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			handler := NewClientHandler(c.Client)
			resp, _, err := handler.Handle(c.Context, NewStackRequest())

			if c.ExpectErr != nil {
				if err == nil {
					t.Fatalf("expect error, got none")
				}
				if err = c.ExpectErr(err); err != nil {
					t.Fatalf("expect error match failed, %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("expect no error, got %v", err)
			}

			if _, ok := resp.(*Response); !ok {
				t.Fatalf("expect Response type, got %T", resp)
			}
		})
	}

}

// TestClientHandler_CloseBodyBeforeTransportFinishesWrite covers a response
// that arrives before net/http's write goroutine is done with the request
// body. After sending ContentLength bytes, net/http copies from the body once
// more to check for extra bytes. Handle closes the body as soon as Do returns,
// so that check can run on the closed body. If the check fails, net/http
// closes the connection while the response body is still being read.
func TestClientHandler_CloseBodyBeforeTransportFinishesWrite(t *testing.T) {
	const failsafe = 10 * time.Second
	reqBody := bytes.Repeat([]byte("a"), 1024)
	respBody := bytes.Repeat([]byte("b"), 64*1024)

	respBodyGate := make(chan struct{})
	sendRespBody := sync.OnceFunc(func() { close(respBodyGate) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Errorf("server: expect to read the request body, got %v", err)
		}
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-respBodyGate
		w.Write(respBody)
	}))
	defer server.Close()
	defer sendRespBody()

	writerGate := make(chan struct{})
	unparkWriter := sync.OnceFunc(func() { close(writerGate) })
	defer unparkWriter()
	connClosedCh := make(chan struct{})
	connClosed := sync.OnceFunc(func() { close(connClosedCh) })

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &parkingConn{Conn: conn, bodyLen: len(reqBody), park: writerGate, onClose: connClosed}, nil
		},
	}
	defer transport.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), failsafe)
	defer cancel()
	wrote := make(chan error, 1)
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) { wrote <- info.Err },
	})

	req := NewStackRequest().(*Request)
	req.Method = http.MethodPost
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}
	req.URL = u
	req, err = req.SetStream(bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}

	handler := NewClientHandlerWithOptions(&http.Client{Transport: transport})
	out, _, err := handler.Handle(ctx, req)
	if err != nil {
		t.Fatalf("expect no error, got %v", err)
	}
	resp := out.(*Response)
	defer resp.Body.Close()

	// Handle has returned, so it has closed the request body. Let net/http
	// check the body for extra bytes now.
	unparkWriter()
	select {
	case err := <-wrote:
		if err != nil {
			t.Errorf("expect the request write to succeed, got %v", err)
			// net/http closes the connection after a failed write. Wait for
			// that so that the read below sees it.
			select {
			case <-connClosedCh:
			case <-time.After(failsafe):
			}
		}
	case <-time.After(failsafe):
		t.Fatal("timed out waiting for the request write to finish")
	}

	sendRespBody()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("expect to read the response body, got %v", err)
	}
	if e, a := len(respBody), len(got); e != a {
		t.Errorf("expect %d response body bytes, got %d", e, a)
	}
}

// parkingConn blocks net/http's write goroutine right after the last byte of
// the request body reaches the socket, until park is closed. At that point the
// server has the whole request, and net/http has not yet checked the body for
// extra bytes. The conn is meant to carry a single request.
type parkingConn struct {
	net.Conn
	bodyLen int
	park    <-chan struct{}
	onClose func()

	sent   []byte
	parked bool
}

func (c *parkingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.sent = append(c.sent, p[:n]...)
	if !c.parked {
		if i := bytes.Index(c.sent, []byte("\r\n\r\n")); i >= 0 && len(c.sent)-(i+4) >= c.bodyLen {
			c.parked = true
			<-c.park
		}
	}
	return n, err
}

func (c *parkingConn) Close() error {
	c.onClose()
	return c.Conn.Close()
}
