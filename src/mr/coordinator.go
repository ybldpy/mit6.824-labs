package mr

import (
	"fmt"
	"log"
	"path/filepath"
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
	workerId   int
}

type Coordinator struct {
	// Your definitions here.
	mapTasks         []Task
	mapTaskLock      sync.Mutex
	mapTaskDoneCount int
	mapTaskDoneLock  sync.Mutex

	reduceTasks         []Task
	reduceTaskLock      sync.Mutex
	reduceTaskDoneCount int
	reduceTaskDoneLock  sync.Mutex

	//intermediateFiles map[int][]string

	workers     map[int]int64
	workersLock sync.Mutex

	//executingTasks map[int]*Task
	//executingTasksLock sync.Mutex

	workerIndex   int
	workerIdxLock sync.Mutex

	overtimeLimit int64
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Register(request *struct{}, response *RegisterResponse) error {
	c.workerIdxLock.Lock()
	id := c.workerIndex
	c.workerIndex += 1

	c.workerIdxLock.Unlock()

	c.workersLock.Lock()
	c.workers[id] = time.Now().Unix()
	c.workersLock.Unlock()

	response.State = STATE_OK
	response.WorkerId = id
	response.NReduce = len(c.reduceTasks)
	return nil
}

//func (c *Coordinator) TaskDone(request *TaskDoneRequest) error {
//}

func (c *Coordinator) removeDeadWorker() {

	c.workersLock.Lock()
	defer c.workersLock.Unlock()
	var deadWorkers []int = make([]int, 0, len(c.workers))
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

func (c *Coordinator) maintainActiveWorker() {
	for true {
		c.removeDeadWorker()
		time.Sleep(time.Second * 2)
	}
}

func (c *Coordinator) isValidWorker(wid int) bool {
	c.workersLock.Lock()
	defer c.workersLock.Unlock()
	_, exists := c.workers[wid]
	return exists
}

func (c *Coordinator) updateWorkerActive(wid int) {

	//fmt.Printf("KEEPALIVE: WORKER: %d\n", wid)

	c.workersLock.Lock()
	defer c.workersLock.Unlock()
	c.workers[wid] = time.Now().Unix()
}

func (c *Coordinator) trackSlowWorker(task *Task, lock *sync.Mutex) {

	time.Sleep(10 * time.Second)

	lock.Lock()
	if task.taskState == 1 {
		task.taskState = 0
	}
	lock.Unlock()

}

func (c *Coordinator) KeepAlive(workerID *int, response *BaseResponse) error {

	if !c.isValidWorker(*workerID) {
		response.State = STATE_UNKNOWN
		return nil
	}
	c.updateWorkerActive(*workerID)
	response.State = STATE_OK
	return nil
}

func (c *Coordinator) allocateIdleTask(tasks *[]Task, locker *sync.Mutex, wid int, workerTask *WorkerTask) {
	locker.Lock()
	defer locker.Unlock()
	for i := range *tasks {
		if (*tasks)[i].taskState == 0 {
			workerTask.TaskType = (*tasks)[i].taskType
			workerTask.TaskId = (*tasks)[i].taskId
			workerTask.MapTaskKey = (*tasks)[i].mapTaskKey
			if (*tasks)[i].taskType == 1 {
				intermediateFiles := make([]string, len(c.mapTasks))
				for j := 0; j < len(c.mapTasks); j++ {
					intermediateFiles[j] = filepath.Join("mr-tmp", fmt.Sprintf("mr-%d-%d", j, (*tasks)[i].taskId))
				}
				workerTask.IntermediateFileLocations = intermediateFiles
			}
			(*tasks)[i].taskState = 1
			go c.trackSlowWorker(&(*tasks)[i], locker)
			return
		}
	}

}

func (c *Coordinator) GetTask(workerID *int, response *RequestTaskResponse) error {
	if !c.isValidWorker(*workerID) {
		response.State = STATE_UNKNOWN
		return nil
	}
	c.updateWorkerActive(*workerID)
	if c.Done() {
		response.AllTaskDone = true
		return nil
	}
	response.State = STATE_OK
	var workerTask WorkerTask = WorkerTask{-1, -1, "", make([]string, 0)}

	c.mapTaskDoneLock.Lock()
	defer c.mapTaskDoneLock.Unlock()
	if c.mapTaskDoneCount < len(c.mapTasks) {
		c.allocateIdleTask(&c.mapTasks, &c.mapTaskLock, *workerID, &workerTask)
		response.Task = workerTask
		return nil
	}

	c.reduceTaskDoneLock.Lock()
	defer c.reduceTaskDoneLock.Unlock()
	if c.reduceTaskDoneCount < len(c.reduceTasks) {
		c.allocateIdleTask(&c.reduceTasks, &c.reduceTaskLock, *workerID, &workerTask)
	}

	response.Task = workerTask
	return nil
}

func (c *Coordinator) TaskDone(request *TaskDoneRequest, response *BaseResponse) error {
	if !c.isValidWorker(request.Wid) {
		response.State = STATE_UNKNOWN
		return nil
	}

	c.updateWorkerActive(request.Wid)

	var task *Task = nil
	if request.State == -1 {
		if request.WorkerTask.TaskType == 0 {
			c.mapTaskLock.Lock()
			c.mapTasks[request.WorkerTask.TaskId].taskState = 0
			c.mapTaskLock.Unlock()
		} else {
			c.reduceTaskLock.Lock()
			c.reduceTasks[request.WorkerTask.TaskId].taskState = 0
			c.reduceTaskLock.Unlock()
		}
		response.State = STATE_OK
		return nil
	} else if request.WorkerTask.TaskType == 0 {
		task = &c.mapTasks[request.WorkerTask.TaskId]
	} else {
		task = &c.reduceTasks[request.WorkerTask.TaskId]
	}
	c.processTaskDone(task, request.Wid)
	response.State = STATE_OK
	return nil
}

func (c *Coordinator) processTaskDone(task *Task, wid int) {

	//c.executingTasksLock.Lock()
	//var executingTask *Task = c.executingTasks[wid]
	//if executingTask == nil {
	//	c.executingTasksLock.Unlock()
	//	return nil
	//}
	//delete(c.executingTasks, wid)
	//c.executingTasksLock.Unlock()

	lock := &c.mapTaskDoneLock
	count := &c.mapTaskDoneCount
	countLock := &c.mapTaskDoneLock
	if task.taskType == 1 {
		lock = &c.reduceTaskDoneLock
		count = &c.reduceTaskDoneCount
		countLock = &c.reduceTaskDoneLock
	}

	completed := false
	lock.Lock()
	if task.taskState == 1 {
		task.taskState = 2
		completed = true
	}
	lock.Unlock()

	if completed {
		countLock.Lock()
		defer countLock.Unlock()
		*count += 1
	}

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

	//fmt.Printf("[LOG]:  %d %d\n", c.mapTaskDoneCount, c.reduceTaskDoneCount)
	if c.mapTaskDoneCount < len(c.mapTasks) {
		c.mapTaskDoneLock.Unlock()
		return false
	}
	c.mapTaskDoneLock.Unlock()
	c.reduceTaskDoneLock.Lock()
	if c.reduceTaskDoneCount < len(c.reduceTasks) {
		c.reduceTaskDoneLock.Unlock()
		return false
	}
	c.reduceTaskDoneLock.Unlock()
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
		mapTasks:      mapTasks,
		reduceTasks:   reduceTasks,
		workers:       make(map[int]int64),
		overtimeLimit: int64(10 * 1000 * 1000 * 1000),
	}

	go c.maintainActiveWorker()
	return &c

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	//fmt.Println(files)

	c := initCoordinator(files, nReduce)
	// Your code here.
	c.server()

	return c
}
