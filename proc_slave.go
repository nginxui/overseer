package overseer

import (
	"log"
	"net"
	"os"
	"os/signal"
	"time"
)

var (
	// DisabledState is a placeholder state for when overseer is disabled and the program function is run manually.
	DisabledState = State{Enabled: false}
)

// State contains the current run-time state of overseer
type State struct {
	// whether overseer is running enabled. When enabled,
	//this program will be running in a child process and
	//overseer will perform rolling upgrades.
	Enabled bool

	// ID is an SHA-1 hash of the current running binary
	ID string

	// StartedAt records the start time of the program
	StartedAt time.Time

	// Listener is the first net.Listener in Listeners
	Listener net.Listener

	// Listeners are the set of acquired sockets by the master process.
	// These are all passed into this program in the same order they are specified in Config.Addresses.
	Listeners []net.Listener

	// Address for program's first listening
	Address string

	// Addresses for program's listening
	Addresses []string

	// GracefulShutdown will be filled when it's time to perform a graceful shutdown.
	GracefulShutdown chan bool

	// BinPath is path of the binary currently being executed
	BinPath string
}

//a overseer slave process

type slave struct {
	*Config
	id         string
	listeners  []*overseerListener
	masterPid  int
	masterProc *os.Process
	state      State
}

func (sp *slave) run() error {
	sp.id = os.Getenv(envSlaveID)
	sp.debugf("run")
	sp.state.Enabled = true
	sp.state.ID = os.Getenv(envBinID)
	sp.state.StartedAt = time.Now()
	sp.state.Address = sp.Config.Address
	sp.state.Addresses = sp.Config.Addresses
	sp.state.GracefulShutdown = make(chan bool, 1)
	sp.state.BinPath = os.Getenv(envBinPath)
	if err := sp.watchParent(); err != nil {
		return err
	}
	if err := sp.initFileDescriptors(); err != nil {
		return err
	}
	sp.watchSignal()
	//run program with state
	sp.debugf("start program")
	sp.Config.Program(sp.state)
	return nil
}

func (sp *slave) watchSignal() {
	signals := make(chan os.Signal)
	signal.Notify(signals, sp.Config.RestartSignal)
	go func() {
		<-signals
		signal.Stop(signals)
		sp.debugf("graceful shutdown requested")
		// master wants to restart,
		close(sp.state.GracefulShutdown)
		// release any sockets and notify master
		if len(sp.listeners) > 0 {
			// perform graceful shutdown
			for _, l := range sp.listeners {
				l.release(sp.Config.TerminateTimeout)
			}
			// signal release of held sockets, allows master to start a new process
			// before this child has actually exited. early restarts not supported with restarts disabled.
			if !sp.NoRestart {
				sp.masterProc.Signal(SIGUSR1)
			}
			// listeners should be waiting on connections to close...
		}
		// start death-timer
		go func() {
			time.Sleep(sp.Config.TerminateTimeout)
			sp.debugf("timeout. forceful shutdown")
			os.Exit(1)
		}()
	}()
}

func (sp *slave) triggerRestart() {
	if err := sp.masterProc.Signal(sp.Config.RestartSignal); err != nil {
		os.Exit(1)
	}
}

func (sp *slave) debugf(f string, args ...interface{}) {
	if sp.Config.Debug {
		log.Printf("[overseer slave#"+sp.id+"] "+f, args...)
	}
}

func (sp *slave) warnf(f string, args ...interface{}) {
	if sp.Config.Debug || !sp.Config.NoWarn {
		log.Printf("[overseer slave#"+sp.id+"] "+f, args...)
	}
}
