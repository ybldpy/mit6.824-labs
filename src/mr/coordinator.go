package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	srcFiles          []string
	nReduce           int
	intermediateFiles []string

	workers     map[int]int64
	workersLock sync.Mutex

	workerIndex   int
	workerIdxLock sync.Mutex

	overtimeLimit int64
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) register() error {
	return nil
}

func (c *Coordinator) removeDeadWorker() {

	var deadWorkers []int = make([]int, 0, len(c.workers))
	c.workersLock.Lock()
	defer c.workersLock.Unlock()
	var currentTimestamp int64 = time.Now().Unix()
	for wid, lastRequestTimestamp := range c.workers {
		if currentTimestamp-lastRequestTimestamp > c.overtimeLimit {
			deadWorkers = append(deadWorkers, wid)
		}
	}
	for _, wid := range deadWorkers {
		delete(c.workers, wid)
	}
}

func (c *Coordinator) isValidWorker(wid int) bool {
	c.workersLock.Lock()
	defer c.workersLock.Unlock()
	_, exists := c.workers[wid]
	return exists
}

func (c *Coordinator) keepAlive(workerID int, response *BaseResponse) error {

	if !c.isValidWorker(workerID) {
		response.state = STATE_UNKNOWN
		return nil
	}
	c.workers[workerID] = time.Now().Unix()
	response.state = STATE_OK
	return nil
}

func (c *Coordinator) getTask(workerID int, response *RequestTaskResponse) error {
	if !c.isValidWorker(workerID) {
		response.state = STATE_UNKNOWN
		return nil
	}

}

func (c *Coordinator) mapTaskDone() error {
	return nil
}

func (c *Coordinator) reduceTaskDone() error {
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files, nReduce, make([]string, 0), make(map[int]int64), 0, 10}
	// Your code here.

	c.server()
	return &c
}
