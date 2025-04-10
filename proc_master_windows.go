//go:build windows

package overseer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// forwarder is used to forward an incoming net.Conn to another actual handler.
// Either local (in parent case) or via a Dialer (in child case).
type forwarder struct {
	handle func(net.Conn)
	close  func()
	wg     sync.WaitGroup
}

func (f *forwarder) closeAndWait() {
	f.close()
	f.wg.Wait()
}

type wgCloser struct {
	net.Conn
	done func()
}

func (wc wgCloser) Close() error {
	err := wc.Conn.Close()
	if err == nil || !errors.Is(err, net.ErrClosed) {
		wc.done()
	}
	return err
}

type childRequest struct {
	Addresses []string `json:"addresses"`
}

func (mp *master) retrieveFileDescriptors() (err error) {
	mp.slaveExtraFiles = make([]*os.File, len(mp.Config.Addresses))
	listeners := make([]net.Listener, 0, len(mp.Config.Addresses))

	// Create unique named pipes for each address
	pipeNames := make([]string, len(mp.Config.Addresses))
	pipeListeners := make([]net.Listener, len(mp.Config.Addresses))

	// Track all active connections for cleanup
	var activePipeConns sync.Map

	// Create close function to clean up all connections when process exits
	closePipes := func() {
		mp.debugf("Closing all named pipe connections")
		activePipeConns.Range(func(key, value interface{}) bool {
			if conn, ok := value.(net.Conn); ok {
				conn.Close()
			}
			return true
		})
	}

	// Save cancel function
	originalCancel := mp.pipeCancel
	mp.pipeCancel = func() {
		if originalCancel != nil {
			originalCancel()
		}
		closePipes()
	}

	// Listen for system termination signals
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		mp.debugf("Received termination signal, cleaning up resources")
		closePipes()
	}()

	// Create all pipe listeners
	for i, addr := range mp.Config.Addresses {
		// Create unique pipe name for each address
		pipeName := fmt.Sprintf("overseer_%d_%s_%d.pipe", os.Getpid(),
			hex.EncodeToString([]byte(addr))[0:8], i)

		// Create named pipe listener
		pipeListener, err := listenPipe(pipeName)
		if err != nil {
			// Close already created pipes
			for j := 0; j < i; j++ {
				pipeListeners[j].Close()
			}
			return fmt.Errorf("failed to create named pipe for address %s: %v", addr, err)
		}

		pipeNames[i] = pipeName
		pipeListeners[i] = pipeListener
		mp.debugf("Created named pipe for address %s", addr)
	}

	// Save pipe names to pass as environment variables to child process
	mp.pipeNames = pipeNames

	// Create cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	mp.pipeCancel = cancel

	// Create TCP listeners and corresponding forwarders for each address
	for i, addr := range mp.Config.Addresses {
		// Create TCP listener
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("failed to listen on address %s: %v", addr, err)
		}
		listeners = append(listeners, ln)
		mp.debugf("Created TCP listener for address %s", addr)

		// Create connection channel
		connCh := make(chan net.Conn, 10)
		forwarderCh := make(chan *forwarder, 1)

		// Start external listener
		go mp.runExternalListener(ln, connCh)

		// Start internal listener
		go mp.runInternalListener(connCh, forwarderCh, func() {
			mp.debugf("Internal listener for address %s closed", addr)
		})

		// Wait in background for child process to connect to the corresponding pipe
		go func(i int, addr string, pipeListener net.Listener, forwarderCh chan *forwarder) {
			defer pipeListener.Close()
			mp.debugf("Waiting for child process to connect to named pipe for address %s", addr)

			// Listen for context cancellation
			go func() {
				<-ctx.Done()
				pipeListener.Close()
			}()

			// Accept connection
			conn, err := pipeListener.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				mp.debugf("Error accepting pipe connection for address %s: %v", addr, err)
				return
			}

			// Store connection for cleanup
			connID := fmt.Sprintf("pipe-%s-%d", addr, i)
			activePipeConns.Store(connID, conn)

			mp.debugf("Accepted pipe connection from child process for address %s", addr)

			// Create forwarder
			fw := &forwarder{
				close: func() {
					conn.Close()
					activePipeConns.Delete(connID)
				},
			}

			// Set handler function
			fw.handle = func(clientConn net.Conn) {
				// Add to wait group
				fw.wg.Add(1)
				wc := wgCloser{
					Conn: clientConn,
					done: fw.wg.Done,
				}

				// Start data forwarding - directly use pipe connection
				go proxyConnection(conn, wc, mp.debugf)
			}

			// Send forwarder to internal listener
			forwarderCh <- fw

			// Keep connection open until main process closes
			<-ctx.Done()
			conn.Close()
			activePipeConns.Delete(connID)
		}(i, addr, pipeListeners[i], forwarderCh)
	}

	return nil
}

// proxyConnection transfers data bidirectionally between two connections
func proxyConnection(conn1, conn2 net.Conn, debug func(string, ...interface{})) {
	// Create control channel
	done := make(chan struct{})

	// From conn1 to conn2
	go func() {
		_, err := io.Copy(conn2, conn1)
		if err != nil {
			if err != io.EOF && !isConnectionClosed(err) {
				debug("Data transfer error: %v", err)
			}
		}
		conn2.Close()
		close(done)
	}()

	// From conn2 to conn1
	go func() {
		_, err := io.Copy(conn1, conn2)
		if err != nil {
			if err != io.EOF && !isConnectionClosed(err) {
				debug("Data transfer error: %v", err)
			}
		}
		conn1.Close()
	}()
}

// isConnectionClosed determines if an error is a connection closed error
func isConnectionClosed(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	if strings.Contains(err.Error(), "use of closed network connection") {
		return true
	}
	if strings.Contains(err.Error(), "connection reset by peer") {
		return true
	}
	if strings.Contains(err.Error(), "broken pipe") {
		return true
	}
	return false
}

func (mp *master) runExternalListener(ln net.Listener, ch chan net.Conn) {
	defer close(ch)
	for {
		rw, err := ln.Accept()

		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			return
		}
		ch <- rw
	}
}

func (mp *master) runInternalListener(connCh chan net.Conn, forwarderCh chan *forwarder, done func()) {
	defer done()

	var current *forwarder
	defer func() {
		if current != nil {
			current.closeAndWait()
		}
	}()

	for {
		select {
		case conn, ok := <-connCh:
			if !ok {
				// connCh closed, we're shutting down
				return
			}
			if current != nil {
				current.handle(conn)
			} else {
				// No child process, close connection directly
				conn.Close()
			}
		case fw, ok := <-forwarderCh:
			if !ok {
				// forwarderCh closed, we're shutting down
				return
			}
			if current != nil {
				current.closeAndWait()
			}
			current = fw
		}
	}
}

// provide the slave process with some state
func (mp *master) retrieveSlaveEnviron() []string {
	e := os.Environ()
	e = append(e, envBinID+"="+hex.EncodeToString(mp.binHash))
	e = append(e, envBinPath+"="+mp.binPath)
	e = append(e, envSlaveID+"="+strconv.Itoa(mp.slaveID))
	e = append(e, envIsSlave+"=1")

	// Add all pipe names to environment variables
	for i, pipeName := range mp.pipeNames {
		e = append(e, fmt.Sprintf("OVERSEER_PIPE_NAME_%d=%s", i, pipeName))
	}
	e = append(e, fmt.Sprintf("OVERSEER_PIPE_COUNT=%d", len(mp.pipeNames)))

	return e
}
