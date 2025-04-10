//go:build !windows

package overseer

import (
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strconv"
)

func (mp *master) retrieveFileDescriptors() error {
	mp.slaveExtraFiles = make([]*os.File, len(mp.Config.Addresses))
	for i, addr := range mp.Config.Addresses {
		a, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			return fmt.Errorf("invalid address %s (%s)", addr, err)
		}
		l, err := net.ListenTCP("tcp", a)
		if err != nil {
			return err
		}
		f, err := l.File()
		if err != nil {
			return fmt.Errorf("failed to retrieve fd for: %s (%s)", addr, err)
		}
		if err := l.Close(); err != nil {
			return fmt.Errorf("failed to close listener for: %s (%s)", addr, err)
		}
		mp.slaveExtraFiles[i] = f
	}
	return nil
}

// provide the slave process with some state
func (mp *master) retrieveSlaveEnviron() []string {
	e := os.Environ()
	e = append(e, envBinID+"="+hex.EncodeToString(mp.binHash))
	e = append(e, envBinPath+"="+mp.binPath)
	e = append(e, envSlaveID+"="+strconv.Itoa(mp.slaveID))
	e = append(e, envIsSlave+"=1")
	e = append(e, envNumFDs+"="+strconv.Itoa(len(mp.slaveExtraFiles)))
	return e
}
