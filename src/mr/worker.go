package mr

import (
	"fmt"
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



func (this *MapReduceWorker) requestTask()  {
	this.sendPingRequest = false

}

//return work state
//-1 network error
func (this *MapReduceWorker) start() int{

	var registerResult bool = this.register();
	if !registerResult {
		return -1;
	}

	this.requestTask()


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
