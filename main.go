package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// CopyFromSource is the interface for the source of rows for CopyFrom.
type CopyFromSource interface {
	Next() bool
	Values() ([]interface{}, error)
	Err() error
}

// MockConn is a net.Conn wrapper that allows simulating network failures.
type MockConn struct {
	net.Conn
	mu           sync.Mutex
	writeCount   int
	failAfter    int
	closed       bool
	writeTimeout time.Duration
}

func (m *MockConn) Write(b []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	m.writeCount++
	if m.failAfter > 0 && m.writeCount >= m.failAfter {
		m.closed = true
		return 0, errors.New("connection reset by peer")
	}
	return len(b), nil
}

func (m *MockConn) Read(b []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, io.EOF
	}
	// Simulate server response for COPY protocol
	return 0, io.EOF
}

func (m *MockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// CopyFrom simulates the copy protocol loop with proper error handling.
func CopyFrom(ctx context.Context, conn net.Conn, rowSrc CopyFromSource) (int64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	var rowsAffected int64
	for rowSrc.Next() {
		select {
		case <-ctx.Done():
			return rowsAffected, ctx.Err()
		default:
		}

		values, err := rowSrc.Values()
		if err != nil {
			return rowsAffected, err
		}

		// Simulate writing CopyData message
		payload := fmt.Sprintf("row-%d-%v", rowsAffected, values)
		_, err = conn.Write([]byte(payload))
		if err != nil {
			return rowsAffected, fmt.Errorf("write CopyData failed: %w", err)
		}
		rowsAffected++
	}

	if err := rowSrc.Err(); err != nil {
		return rowsAffected, err
	}

	// Simulate writing CopyDone message
	_, err := conn.Write([]byte("CopyDone"))
	if err != nil {
		return rowsAffected, fmt.Errorf("write CopyDone failed: %w", err)
	}

	return rowsAffected, nil
}

func main() {
	fmt.Println("Hello, Bounty Hunter!")
}
