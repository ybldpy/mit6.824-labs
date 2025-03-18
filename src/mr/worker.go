package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
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
	register       = "Coordinator.register"
	keepAlive      = "Coordinator.keepAlive"
	getTask        = "Coordinator.getTask"
	mapTaskDone    = "Coordinator.mapTaskDone"
	reduceTaskDone = "Coordinator.reduceTaskDone"
	taskDone       = "Coordinator.taskDone"
	queryIntermediateFileLocations = "Coordinator.queryIntermediateFileLocations"
)

func registerWorker() RegisterResponse {
	var response RegisterResponse = RegisterResponse{BaseResponse{STATE_ERROR},-1,-1};
	call(register, &struct {}{}, &response);
	return response
}




var mapF func(string, string) []KeyValue = nil
var reduceF func(string, []string) string = nil


type MapReduceWorker struct {
	workerId int
	//0 idle 1-in progress 2-completed
	//-1 offline
	state int
	nReduce int
	sendPingRequest bool
	stateChannel chan int
}

func (this *MapReduceWorker) register() bool{
	var retryTimes int = 3
	for retryTimes>0{
		var response RegisterResponse = registerWorker();
		if(response.BaseResponse.state!=STATE_OK){retryTimes-=1}
		else {
			this.workerId = response.workerId
			this.nReduce = response.nReduce
			this.state = 0
			return true;
		}
	}
	return false;
}



func (this *MapReduceWorker) taskDone(task *WorkerTask,state int){


	taskDoneRequest:=TaskDoneRequest{workerTask: *task,state: state}
	response := BaseResponse{}
	call(taskDone,&taskDoneRequest,&response)



}


//stop everything
func (this *MapReduceWorker) offLine()  {



}

func (this *MapReduceWorker) pingRequest(workerId int){
	for this.sendPingRequest{
			keepAliveRound:=2.5*float64(time.Second);
			var response BaseResponse = BaseResponse{STATE_ERROR};
			var ok bool = call(keepAlive, this.workerId, &response);
			if (ok && response.state==STATE_UNKNOWN){
				//offline
				this.state = -1;
				this.offLine();
			}
			time.Sleep(time.Duration(keepAliveRound));
		}
	}
}

func (this *MapReduceWorker) doMap(task *WorkerTask) int{
	this.sendPingRequest = true

	var key string = task.taskParams

	data,e := ioutil.ReadFile(key)
	if e != nil{
		this.sendPingRequest = false
		return -1
	}

	var keyValues []KeyValue = mapF(key,string(data))

	var intermidateFiles map[int]*os.File = make(map[int]*os.File)
	var encoders map[int]*json.Encoder = make(map[int]*json.Encoder)

	for i:=0;i<this.nReduce;i++{
		filename := filepath.Join("mr-tmp",
			fmt.Sprintf("mr-%d-%d", task.taskId, i))
		os.MkdirAll("mr-tmp", 0777)
		f,err := os.Create(filename)
		if err != nil{
			return -1
		}
		defer f.Close()
		intermidateFiles[i] = f
		encoders[i] = json.NewEncoder(f)
	}

	for _,kv:=range keyValues{
		h := fnv.New32a()
		h.Write([]byte(kv.Key))
		partition := int(h.Sum32()) % this.nReduce
		if err := encoders[partition].Encode(&kv); err != nil {
			return -1
		}
	}
	this.sendPingRequest = false
	return 0
}

func (this *MapReduceWorker) queryIntermediateFilesLocations(reduceNumber int) []string  {


}




func (this *MapReduceWorker) doReduce(task *WorkerTask) int {

	files := this.queryIntermediateFilesLocations(task.taskId)
	keyValues := make(map[string][]string)
	sortedKeys := make([]string, 0)

	for i:=0;i<len(files);i++{
		f,e := os.Open(files[i])
		if e!=nil{
			return -1
		}

		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			var kv KeyValue = KeyValue{}
			json.Unmarshal([]byte(line),&kv)
			key := kv.Key
			if _,ok := keyValues[key]; !ok {
				sortedKeys = append(sortedKeys, key)
			}
			keyValues[key]=append(keyValues[key],kv.Value)
		}
	}


	sort.Strings(sortedKeys)
	outputFile, err := os.Create(fmt.Sprintf("mr-out-%d", task.taskId))

	if err != nil {
		return -1
	}
	defer outputFile.Close()
	for i:=0;i<len(sortedKeys);i++{
		output:=reduceF(sortedKeys[i],keyValues[sortedKeys[i]])
		fmt.Fprintf(outputFile,"%v %v\n",sortedKeys[i],output)
	}
	return 0






}


func (this *MapReduceWorker) requestTask(task *WorkerTask) int {
	this.sendPingRequest = false

	getTaskResponse :=RequestTaskResponse{}
	ok := call(getTask, this.workerId, &getTaskResponse);
	if !ok{
		return 1
	}
	if getTaskResponse.state ==STATE_UNKNOWN{
		return 2
	}

	*task = getTaskResponse.task
	return 0

}

//return work state
//0: finish, 1:restart
func (this *MapReduceWorker) start() int{



	var registerResult bool = this.register();
	if !registerResult {
		return 0;
	}


	for true{

		var task WorkerTask = WorkerTask{-1,-1,"",false};
		requestState := this.requestTask(&task)
		if requestState !=0 || task.allTaskDone{
			return requestState
		}
		if !task.allTaskDone {
			this.sendPingRequest = true
			time.Sleep(3*time.Second);
			continue;
		}
		var taskState int = -1
		if task.taskType == 0 {
			taskState = this.doMap(&task);
		}
		else {
			taskState = this.doReduce(&task);
		}

		this.taskDone(&task, taskState)

	}

}


// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	mapF = mapf
	reduceF = reducef
	// Your worker implementation here.
	var worker MapReduceWorker= MapReduceWorker{-1,-1,-1,false}
	// uncomment to send the Example RPC to the coordinator.
	worker.start();
	// CallExample()

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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
