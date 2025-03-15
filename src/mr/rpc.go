package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RegisterResponse struct {
	BaseResponse
	workerId int
	nReduce  int
}

const (
	STATE_UNKNOWN int = 403
	STATE_OK      int = 200
	STATE_ERROR   int = 500
	STATE_REQUEST_FAIL 501
)

type BaseResponse struct {
	state int
}

type PingResponse struct {
	BaseResponse
}


type RequestTaskResponse struct {
	BaseResponse
	//1-map, 2-reduce
	taskType int
	mapTaskKey string
	mapTaskNumber int

	reduceTaskNumber int
	reduceTaskIntermediateFiles []string
}

// Add your RPC definitions here.
type CoordinateFunc interface {
	keepAlive(workerID int) error
	register() error
	getTask() error
	mapTaskDone() error
	reduceTaskDone() error
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
