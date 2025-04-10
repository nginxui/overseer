package overseer

// overseer listeners and connections allow graceful restarts by tracking when all connections
// from a listener have been closed

import (
	"net"
	"os"
	"sync"
	"time"
)

func newOverseerListener(l net.Listener) *overseerListener {
	return &overseerListener{
		Listener:     l,
		closeByForce: make(chan bool),
	}
}

// gracefully closing net.Listener
type overseerListener struct {
	net.Listener
	closeError   error
	closeByForce chan bool
	wg           sync.WaitGroup
}

func (l *overseerListener) Accept() (net.Conn, error) {
	var conn net.Conn

	// Try to convert the listener to TCPListener for better connection control
	if tcpL, ok := l.Listener.(*net.TCPListener); ok {
		tcpConn, tcpErr := tcpL.AcceptTCP()
		if tcpErr != nil {
			return nil, tcpErr
		}
		tcpConn.SetKeepAlive(true)                  // see http.tcpKeepAliveListener
		tcpConn.SetKeepAlivePeriod(3 * time.Minute) // see http.tcpKeepAliveListener
		conn = tcpConn
	} else {
		// For non-TCP listeners, use standard Accept
		standardConn, standardErr := l.Listener.Accept()
		if standardErr != nil {
			return nil, standardErr
		}
		conn = standardConn
	}

	uconn := overseerConn{
		Conn:   conn,
		wg:     &l.wg,
		closed: make(chan bool),
	}
	go func() {
		//connection watcher
		select {
		case <-l.closeByForce:
			uconn.Close()
		case <-uconn.closed:
			// closed manually
		}
	}()
	l.wg.Add(1)
	return uconn, nil
}

// non-blocking trigger close
func (l *overseerListener) release(timeout time.Duration) {
	// stop accepting connections - release fd
	l.closeError = l.Listener.Close()
	// start timer, close by force if deadline not met
	waited := make(chan bool)
	go func() {
		l.wg.Wait()
		waited <- true
	}()
	go func() {
		select {
		case <-time.After(timeout):
			close(l.closeByForce)
		case <-waited:
			//no need to force close
		}
	}()
}

// Close after blocking wait
func (l *overseerListener) Close() error {
	l.wg.Wait()
	return l.closeError
}

func (l *overseerListener) File() *os.File {
	// returns a dup(2) - FD_CLOEXEC flag *not* set
	if tcpL, ok := l.Listener.(*net.TCPListener); ok {
		fl, _ := tcpL.File()
		return fl
	}

	// For non-TCP listeners, return nil
	// This is safe because Windows version doesn't use ExtraFiles feature
	return nil
}

// notifying on close net.Conn
type overseerConn struct {
	net.Conn
	wg     *sync.WaitGroup
	closed chan bool
}

func (o overseerConn) Close() error {
	err := o.Conn.Close()
	if err == nil {
		o.wg.Done()
		o.closed <- true
	}
	return err
}
