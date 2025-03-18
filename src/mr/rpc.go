package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"sync"
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

type WorkerTask struct {
	taskType int
	taskId   int
	taskParams string
}


type TaskDoneRequest struct {
	workerTask WorkerTask
	//0-fail 1-success
	state int
	wid int
}


type RequestTaskResponse struct {
	BaseResponse
	task WorkerTask
	allTaskDone bool
}

// Add your RPC definitions here.
type CoordinateFunc interface {
	KeepAlive(workerID int, response *BaseResponse) error
	Register() error
	GetTask(workerID int, response *RequestTaskResponse) error
	AllocateIdleTask(tasks *[]Task, locker *sync.Mutex, wid int, workerTask *WorkerTask) error
	TaskDone(request *TaskDoneRequest) error
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
