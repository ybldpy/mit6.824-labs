package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var workerShutDown bool = false

const (
	register  = "Coordinator.Register"
	keepAlive = "Coordinator.KeepAlive"
	getTask   = "Coordinator.GetTask"
	taskDone  = "Coordinator.TaskDone"
)

func registerWorker() RegisterResponse {
	var response RegisterResponse = RegisterResponse{BaseResponse{STATE_ERROR}, -1, -1}
	call(register, &struct{}{}, &response)
	return response
}

var mapF func(string, string) []KeyValue = nil
var reduceF func(string, []string) string = nil

type MapReduceWorker struct {
	workerId int
	//0 idle 1-in progress 2-completed
	nReduce         int
	sendPingRequest bool
	pingLock        sync.Mutex
}

func (this *MapReduceWorker) register() bool {
	var retryTimes int = 3
	for retryTimes > 0 {
		var response RegisterResponse = registerWorker()

		if response.State != STATE_OK {
			retryTimes -= 1
		} else {
			this.workerId = response.WorkerId
			this.nReduce = response.NReduce
			return true
		}
	}
	return false
}

func (this *MapReduceWorker) taskDone(task *WorkerTask, state int) {

	taskDoneRequest := TaskDoneRequest{WorkerTask: *task, State: state}
	response := BaseResponse{}
	call(taskDone, &taskDoneRequest, &response)

}

func (this *MapReduceWorker) pingRequest(pingPeriod time.Duration) {
	for true {
		for true {
			this.pingLock.Lock()
			if !this.sendPingRequest {
				this.pingLock.Unlock()
				break
			}
			this.pingLock.Unlock()

			var response BaseResponse = BaseResponse{STATE_ERROR}
			var ok bool = call(keepAlive, this.workerId, &response)
			if !ok || response.State == STATE_UNKNOWN {
				return
			}
			time.Sleep(pingPeriod)
		}
		time.Sleep(time.Second)
	}
}

func (this *MapReduceWorker) doMap(task *WorkerTask) int {

	var key string = task.MapTaskKey

	data, e := ioutil.ReadFile(key)
	if e != nil {
		return -1
	}

	var keyValues []KeyValue = mapF(key, string(data))

	var intermidateFiles map[int]*os.File = make(map[int]*os.File)
	var encoders map[int]*json.Encoder = make(map[int]*json.Encoder)

	for i := 0; i < this.nReduce; i++ {
		filename := filepath.Join("mr-tmp",
			fmt.Sprintf("mr-%d-%d", task.TaskId, i))
		os.MkdirAll("mr-tmp", 0777)
		f, err := os.Create(filename)
		if err != nil {
			return -1
		}
		defer f.Close()
		intermidateFiles[i] = f
		encoders[i] = json.NewEncoder(f)
	}

	for _, kv := range keyValues {
		h := fnv.New32a()
		h.Write([]byte(kv.Key))
		partition := int(h.Sum32()) % this.nReduce
		if err := encoders[partition].Encode(&kv); err != nil {
			return -1
		}
	}
	return 0
}

func (this *MapReduceWorker) doReduce(task *WorkerTask) int {

	files := task.IntermediateFileLocations
	keyValues := make(map[string][]string)
	sortedKeys := make([]string, 0)

	for i := 0; i < len(files); i++ {
		f, e := os.Open(files[i])
		if e != nil {
			return -1
		}

		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			var kv KeyValue = KeyValue{}
			json.Unmarshal([]byte(line), &kv)
			key := kv.Key
			if _, ok := keyValues[key]; !ok {
				sortedKeys = append(sortedKeys, key)
			}
			keyValues[key] = append(keyValues[key], kv.Value)
		}
	}

	sort.Strings(sortedKeys)
	outputFile, err := os.Create(fmt.Sprintf("mr-out-%d", task.TaskId))

	if err != nil {
		return -1
	}
	defer outputFile.Close()
	for i := 0; i < len(sortedKeys); i++ {
		output := reduceF(sortedKeys[i], keyValues[sortedKeys[i]])
		fmt.Fprintf(outputFile, "%v %v\n", sortedKeys[i], output)
	}
	return 0

}

func (this *MapReduceWorker) requestTask(task *WorkerTask) int {
	this.pingLock.Lock()
	this.sendPingRequest = false
	this.pingLock.Unlock()

	getTaskResponse := RequestTaskResponse{}
	ok := call(getTask, &this.workerId, &getTaskResponse)
	if !ok || (getTaskResponse.State == STATE_OK && getTaskResponse.AllTaskDone) {
		return 1
	}
	if getTaskResponse.State == STATE_UNKNOWN {
		return 2
	}
	*task = getTaskResponse.Task
	return 0
}

// return work state
// 0: finish, 1:restart
func (this *MapReduceWorker) start() int {

	var registerResult bool = this.register()
	if !registerResult {
		return 1
	}

	for true {

		var task WorkerTask = WorkerTask{-1, -1, "", make([]string, 0)}
		requestState := this.requestTask(&task)
		if requestState != 0 {
			return requestState
		}
		if task.TaskId == -1 {
			time.Sleep(3 * time.Second)
			continue
		}
		var taskState int = -1
		this.pingLock.Lock()
		this.sendPingRequest = true
		this.pingLock.Unlock()
		if task.TaskType == 0 {
			taskState = this.doMap(&task)
		} else {
			taskState = this.doReduce(&task)
		}

		this.pingLock.Lock()
		this.sendPingRequest = false
		this.pingLock.Unlock()
		this.taskDone(&task, taskState)
	}

	return 1
}

func (this *MapReduceWorker) reset() {
	this.pingLock.Lock()
	defer this.pingLock.Unlock()
	this.sendPingRequest = false
	this.nReduce = -1
	this.workerId = -1
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	mapF = mapf
	reduceF = reducef

	// Your worker implementation here.
	var worker MapReduceWorker = MapReduceWorker{-1, -1, false, sync.Mutex{}}
	go worker.pingRequest(3 * time.Second)
	// uncomment to send the Example RPC to the coordinator.
	for worker.start() != 1 {
		worker.reset()
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		//log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
