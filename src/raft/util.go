package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func curMill() int64 {
	return time.Now().Unix()*1000 + int64(time.Now().Nanosecond())/1e6
}
