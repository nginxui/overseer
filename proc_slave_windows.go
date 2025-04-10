//go:build windows

package overseer

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yusufpapurcu/wmi"
)

var (
	Timeout = 3 * time.Second
)

type Win32Process struct {
	Name                  string
	ExecutablePath        *string
	CommandLine           *string
	Priority              uint32
	CreationDate          *time.Time
	ProcessID             uint32
	ThreadCount           uint32
	Status                *string
	ReadOperationCount    uint64
	ReadTransferCount     uint64
	WriteOperationCount   uint64
	WriteTransferCount    uint64
	CSCreationClassName   string
	CSName                string
	Caption               *string
	CreationClassName     string
	Description           *string
	ExecutionState        *uint16
	HandleCount           uint32
	KernelModeTime        uint64
	MaximumWorkingSetSize *uint32
	MinimumWorkingSetSize *uint32
	OSCreationClassName   string
	OSName                string
	OtherOperationCount   uint64
	OtherTransferCount    uint64
	PageFaults            uint32
	PageFileUsage         uint32
	ParentProcessID       uint32
	PeakPageFileUsage     uint32
	PeakVirtualSize       uint64
	PeakWorkingSetSize    uint32
	PrivatePageCount      uint64
	TerminationDate       *time.Time
	UserModeTime          uint64
	WorkingSetSize        uint64
}

// Implement connection to parent process via named pipes
func (sp *slave) initFileDescriptors() error {
	// Get the number of pipes created by parent process
	pipeCountStr := os.Getenv("OVERSEER_PIPE_COUNT")
	if pipeCountStr == "" {
		return fmt.Errorf("missing pipe count environment variable")
	}

	pipeCount, err := strconv.Atoi(pipeCountStr)
	if err != nil {
		return fmt.Errorf("invalid pipe count: %v", err)
	}

	// Verify that pipe count matches address count
	if pipeCount != len(sp.Config.Addresses) {
		return fmt.Errorf("pipe count (%d) does not match address count (%d)",
			pipeCount, len(sp.Config.Addresses))
	}

	// Create virtual listeners
	sp.listeners = make([]*overseerListener, pipeCount)
	sp.state.Listeners = make([]net.Listener, pipeCount)
	sp.debugf("Preparing to create %d virtual listeners", pipeCount)

	// Connect to corresponding pipes and create listeners for each address
	for i, addr := range sp.Config.Addresses {
		// Get pipe name for this address
		pipeName := os.Getenv(fmt.Sprintf("OVERSEER_PIPE_NAME_%d", i))
		if pipeName == "" {
			return fmt.Errorf("missing pipe name for address %s", addr)
		}

		sp.debugf("Trying to connect to pipe for address %s", addr)

		// Connect to pipe
		conn, err := dialPipe(pipeName)
		if err != nil {
			return fmt.Errorf("failed to connect to pipe for address %s: %v", addr, err)
		}

		// Create connection channel for this address
		connCh := make(chan net.Conn, 10)

		// Create virtual listener
		l := &pipeListener{
			addr:   addr,
			connCh: connCh,
		}

		// Create overseerListener wrapper
		u := newOverseerListener(l)
		sp.listeners[i] = u
		sp.state.Listeners[i] = u

		// Start goroutine to handle TCP connections
		go func(addr string, conn net.Conn, connCh chan net.Conn) {
			defer conn.Close()

			// Create connection termination channel
			terminated := make(chan struct{})

			// Goroutine to detect pipe connection status
			go func() {
				// Try simple read, EOF or error indicates the pipe has disconnected
				buf := make([]byte, 1)
				_, err := conn.Read(buf)
				if err != nil {
					if err == io.EOF {
						sp.debugf("Detected pipe EOF, parent process may have exited: %s", addr)
					} else {
						sp.debugf("Detected pipe error, parent process may have exited: %v", err)
					}
					close(terminated)
				}
			}()

			// Connection loop
			connectionAttempts := 0
			for {
				select {
				case <-terminated:
					return
				default:
					// If too many consecutive failures, stop creating new connections
					if connectionAttempts >= 3 {
						sp.debugf("Too many consecutive connection failures for address %s, stopping attempts", addr)
						return
					}

					// Create pipe connection
					pipeConn := &pipeConn{
						reader: conn,
						writer: conn,
						local:  l.Addr(),
						id:     fmt.Sprintf("%s-conn", addr),
						sp:     sp,
						done:   make(chan struct{}),
					}

					// Send to connection channel
					select {
					case connCh <- pipeConn:
						// Wait for connection to complete or close
						select {
						case <-pipeConn.done:
							// Check if closed due to EOF
							if pipeConn.closedByEOF {
								connectionAttempts++
								// Add delay after consecutive EOF errors to avoid generating excessive logs
								time.Sleep(time.Duration(connectionAttempts*500) * time.Millisecond)
							} else {
								// Normal closure, reset attempt counter
								connectionAttempts = 0
							}
						case <-terminated:
							return
						}
					case <-time.After(2 * time.Second):
						// Timeout, pipe may be closed
						connectionAttempts++
						return
					case <-terminated:
						return
					}
				}
			}
		}(addr, conn, connCh)
	}

	// Set main listener
	if len(sp.state.Listeners) > 0 {
		sp.state.Listener = sp.state.Listeners[0]
		sp.debugf("Setting first listener as main listener")
	}

	return nil
}

// pipeListener implements net.Listener interface
type pipeListener struct {
	addr   string
	connCh chan net.Conn
	closed bool
	mu     sync.Mutex
}

func (l *pipeListener) Accept() (net.Conn, error) {
	if conn, ok := <-l.connCh; ok {
		return conn, nil
	}
	return nil, fmt.Errorf("listener closed")
}

func (l *pipeListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.closed {
		l.closed = true
		close(l.connCh)
	}

	return nil
}

func (l *pipeListener) Addr() net.Addr {
	return pipeAddr(l.addr)
}

// pipeAddr implements net.Addr interface
type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }

// pipeConn implements net.Conn interface
type pipeConn struct {
	reader      io.Reader
	writer      io.Writer
	local       net.Addr
	closed      bool
	closedByEOF bool // Mark as closed by EOF
	closeMu     sync.Mutex
	id          string
	sp          *slave
	done        chan struct{} // Signal channel, indicating connection completed/closed
}

// Initialize pipeConn
func newPipeConn(reader io.Reader, writer io.Writer, local net.Addr, id string, sp *slave) *pipeConn {
	return &pipeConn{
		reader: reader,
		writer: writer,
		local:  local,
		id:     id,
		sp:     sp,
		done:   make(chan struct{}),
	}
}

func (c *pipeConn) Read(b []byte) (n int, err error) {
	if c.closed {
		return 0, net.ErrClosed
	}

	n, err = c.reader.Read(b)
	if err != nil && c.sp != nil {
		if err == io.EOF {
			c.closedByEOF = true // Mark as closed by EOF
		}
	}

	return n, err
}

func (c *pipeConn) Write(b []byte) (n int, err error) {
	if c.closed {
		return 0, net.ErrClosed
	}

	n, err = c.writer.Write(b)
	return n, err
}

func (c *pipeConn) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	if !c.closed {
		c.closed = true

		// Notify connection has completed
		select {
		case <-c.done: // Already closed
		default:
			close(c.done)
		}
	}

	return nil
}

func (c *pipeConn) LocalAddr() net.Addr {
	return c.local
}

func (c *pipeConn) RemoteAddr() net.Addr {
	return pipeAddr("pipe-remote")
}

func (c *pipeConn) SetDeadline(t time.Time) error {
	return nil // Not supported
}

func (c *pipeConn) SetReadDeadline(t time.Time) error {
	return nil // Not supported
}

func (c *pipeConn) SetWriteDeadline(t time.Time) error {
	return nil // Not supported
}

func (sp *slave) watchParent() error {
	sp.masterPid = os.Getppid()
	proc, err := os.FindProcess(sp.masterPid)
	if err != nil {
		return fmt.Errorf("master process: %s", err)
	}
	sp.masterProc = proc
	sp.debugf("Found parent process, PID: %d", sp.masterPid)

	go func() {
		sp.debugf("Starting monitoring of parent process (PID: %d) alive status", sp.masterPid)
		// Periodically check if parent process is alive
		failures := 0
		for {
			// Try to get process info via WMI
			_, err := GetWin32Proc(int32(sp.masterPid))
			if err != nil {
				failures++
				sp.debugf("Parent process detection failed (%d/3): %v", failures, err)

				// If it's a WMI error, try alternative methods to check the process
				if failures == 1 {
					// Try using os.FindProcess as a fallback
					if _, err := os.FindProcess(sp.masterPid); err == nil {
						// On Windows, FindProcess almost always succeeds,
						// so we try to send a null signal to check if process exists
						if err := sp.masterProc.Signal(os.Signal(nil)); err == nil {
							sp.debugf("Verified parent process alive using alternative method")
							failures = 0 // Reset failure count
						}
					}
				}

				if failures >= 3 {
					sp.debugf("Parent process has terminated, child process will exit")
					os.Exit(1)
				}
			} else if failures > 0 {
				sp.debugf("Parent process detection returned to normal")
				failures = 0
			}
			time.Sleep(2 * time.Second)
		}
	}()

	return nil
}

func GetWin32Proc(pid int32) ([]Win32Process, error) {
	return GetWin32ProcWithContext(context.Background(), pid)
}

func GetWin32ProcWithContext(ctx context.Context, pid int32) ([]Win32Process, error) {
	var dst []Win32Process
	query := fmt.Sprintf("SELECT * FROM Win32_Process WHERE ProcessId = %d", pid)
	err := WMIQueryWithContext(ctx, query, &dst)
	if err != nil {
		return []Win32Process{}, fmt.Errorf("could not get win32Proc: %s", err)
	}

	if len(dst) == 0 {
		return []Win32Process{}, fmt.Errorf("could not get win32Proc: empty")
	}

	return dst, nil
}

func WMIQueryWithContext(ctx context.Context, query string, dst interface{}, connectServerArgs ...interface{}) error {
	if _, ok := ctx.Deadline(); !ok {
		ctxTimeout, cancel := context.WithTimeout(ctx, Timeout)
		defer cancel()
		ctx = ctxTimeout
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- wmi.QueryNamespace(query, dst, "root\\CIMV2")
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}

// overwrite: see https://github.com/jpillora/overseer/issues/56#issuecomment-656405955
func overwrite(dst, src string) error {
	old := strings.TrimSuffix(dst, ".exe") + "-old.exe"
	if err := move(old, dst); err != nil {
		return err
	}
	if err := move(dst, src); err != nil {
		return err
	}
	os.Remove(old)
	return nil
}
