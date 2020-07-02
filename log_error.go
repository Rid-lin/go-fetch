package main

import (
	"log"
	"os"
)

func fatal(v interface{}) {
	log.Fatalln(v)
	os.Exit(1)
}

func chk(err error) {
	if err != nil {
		fatal(err)
	}
}

func chkM(message string, err error) {
	if err != nil {
		log.Fatalf("%v %v", message, err)
	}
}

func toLog(level int, lvIn int, v ...interface{}) {
	// level - level logging
	// 0 - silent
	// 1 - only panic
	// 2 - panic, warning
	// 3 - panic, warning, access granted and denided
	// lvIn - level log for this event
	if lvIn <= level {
		log.Println(v...)
	}
}
