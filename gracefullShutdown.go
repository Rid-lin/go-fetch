package main

import (
	"os"
	"os/signal"
	"syscall"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

func getExitSignalsChannel() chan os.Signal {

	c := make(chan os.Signal, 1)
	signal.Notify(c,
		// https://www.gnu.org/software/libc/manual/html_node/Termination-Signals.html
		syscall.SIGTERM, // "the normal way to politely ask a program to terminate"
		syscall.SIGINT,  // Ctrl+C
		syscall.SIGQUIT, // Ctrl-\
		// syscall.SIGKILL, // "always fatal", "SIGKILL and SIGSTOP may not be caught by a program"
		// syscall.SIGHUP, // "terminal is disconnected"
	)
	return c

}

func (t *transport) Exit() {
	<-t.exitChan
	t.db.Close()
	if err := os.Remove(config.PIDFileName); err != nil {
		log.Errorf("Error remove file(%v):%v", config.PIDFileName, err)
	} else {
		log.Debugf("PID file(%v) has been successfully deleted", config.PIDFileName)
	}
	log.Println("Shutting down")
	os.Exit(0)

}
