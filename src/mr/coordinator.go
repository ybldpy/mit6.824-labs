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

type Task struct {
	//0 - map 1-reduce
	taskType int
	//0-idle -1 in progress 2-completed
	taskState  int
	taskId     int
	mapTaskKey string

	workerId int
}

type Coordinator struct {
	// Your definitions here.
	mapTasks    []Task
	mapTaskLock sync.Mutex
	mapTaskDoneCount int
	mapTaskDoneLock  sync.Mutex

	reduceTasks    []Task
	reduceTaskLock sync.Mutex
	reduceTaskDoneCount int
	reduceTaskDoneLock  sync.Mutex

	//intermediateFiles map[int][]string

	workers     map[int]int64
	workersLock sync.Mutex

	executingTasks map[int]*Task
	executingTasksLock sync.Mutex

	workerIndex   int
	workerIdxLock sync.Mutex

	overtimeLimit int64
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Register() error {
	return nil
}

func (c *Coordinator) TaskDone(request *TaskDoneRequest) error {

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

func (c *Coordinator) KeepAlive(workerID int, response *BaseResponse) error {

	if !c.isValidWorker(workerID) {
		response.state = STATE_UNKNOWN
		return nil
	}
	c.workers[workerID] = time.Now().Unix()
	response.state = STATE_OK
	return nil
}

func (c *Coordinator) AllocateIdleTask(tasks *[]Task, locker *sync.Mutex, wid int, workerTask *WorkerTask) {
	locker.Lock()
	defer locker.Unlock()
	for i := range *tasks {
		if (*tasks)[i].taskState == 0 {
			workerTask.taskType = (*tasks)[i].taskType
			workerTask.taskId = (*tasks)[i].taskId
			workerTask.taskParams = (*tasks)[i].mapTaskKey
			(*tasks)[i].taskState = 1
			c.executingTasks[wid] = &(*tasks)[i]
		}
	}

}

func (c *Coordinator) GetTask(workerID int, response *RequestTaskResponse) error {
	if !c.isValidWorker(workerID) {
		response.state = STATE_UNKNOWN
		return nil
	}

	if c.Done(){
		response.allTaskDone = true
		return nil
	}


	response.state = STATE_OK
	var workerTask WorkerTask = WorkerTask{-1, -1, ""}
	c.AllocateIdleTask(&c.mapTasks, &c.mapTaskLock, workerID, &workerTask)
	if workerTask.taskType != -1 {
		response.task = workerTask
		return nil
	}
	c.AllocateIdleTask(&c.reduceTasks, &c.reduceTaskLock, workerID, &workerTask)
	response.task = workerTask
	return nil

}

func (c *Coordinator) TaskDone(request *TaskDoneRequest, response *BaseResponse) error  {
	if !c.isValidWorker(request.wid) {
		response.state = STATE_UNKNOWN
		return nil
	}

	task := nil
	if request.workerTask.taskType == 0 {
		task = &c.mapTasks[request.workerTask.taskId]
	}
	else {
		task = &c.reduceTasks[request.workerTask.taskId]
	}
	c.processTaskDone(task,request.wid)
	response.state = STATE_OK
	return nil
}


func (c *Coordinator) processTaskDone(task *Task,wid int) error {

	c.executingTasksLock.Lock()
	var executingTask *Task = c.executingTasks[wid]
	if executingTask == nil {
		c.executingTasksLock.Unlock()
		return nil
	}
	delete(c.executingTasks, wid)
	c.executingTasksLock.Unlock()

	lock:=&c.mapTaskDoneLock
	count:=&c.mapTaskDoneCount
	if task.taskType == 1 {
		lock = &c.reduceTaskDoneLock
		count = &c.reduceTaskDoneCount
	}
	task.taskState = 2
	lock.Lock()
	(*count)+=1
	lock.Unlock()
	return nil;
}

func (c *Coordinator) reduceTaskDone(task WorkerTask) error {
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
	// Your code here.
	c.mapTaskDoneLock.Lock()
	defer c.mapTaskDoneLock.Unlock()
	if c.mapTaskDoneCount < len(c.mapTasks) {
		return false
	}
	c.reduceTaskDoneLock.Lock()
	defer c.reduceTaskDoneLock.Unlock()
	if c.reduceTaskDoneCount < len(c.reduceTasks) {
		return false
	}
	return true
}

func initCoordinator(files []string, nReduce int) *Coordinator {

	var mapTasks []Task = make([]Task, len(files))

	for i := range mapTasks {
		mapTask := &mapTasks[i]
		mapTask.taskType = 0
		mapTask.taskState = 0
		mapTask.taskId = i
		mapTask.mapTaskKey = files[i]
		mapTask.workerId = -1
	}

	var reduceTasks []Task = make([]Task, nReduce)
	for i := range reduceTasks {
		reduceTasks[i].taskType = 1
		reduceTasks[i].taskState = 0
		reduceTasks[i].taskId = i
		reduceTasks[i].workerId = -1
	}

	c := Coordinator{
		mapTasks:          mapTasks,
		reduceTasks:       reduceTasks,
		workers:           make(map[int]int64),
	}

	return &c

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := initCoordinator(files, nReduce)
	// Your code here.
	c.server()

	go c.removeDeadWorker()

	return c
}
