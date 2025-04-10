//go:build windows

package overseer

import (
	"net"

	"github.com/Microsoft/go-winio"
)

// listenPipe creates a Windows named pipe listener
func listenPipe(name string) (net.Listener, error) {
	return winio.ListenPipe(`\\.\pipe\`+name, nil)
}

// dialPipe connects to a Windows named pipe
func dialPipe(name string) (net.Conn, error) {
	return winio.DialPipe(`\\.\pipe\`+name, nil)
}
