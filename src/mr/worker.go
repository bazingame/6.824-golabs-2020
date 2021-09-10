package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for taskInfo := getJob(); taskInfo.TaskType != 0; taskInfo = getJob() {
		log.Printf("get one job from master %v %v ...... \n", taskInfo.TaskType, taskInfo.TaskNum)
		if taskInfo.TaskType == TaskTypeMap {
			handleMapTask(mapf, taskInfo)
		} else if taskInfo.TaskType == TaskTypeReduce {
			handleReduceTask(reducef, taskInfo)
		}
		finishJob(taskInfo)
	}
}

func handleMapTask(mapf func(string, string) []KeyValue, taskInfo GetTaskReply) {
	log.Printf("handling map task %v ...... \n", taskInfo.MapTaskInfo.FileName)
	content := readFile(taskInfo.MapTaskInfo.FileName)
	kva := mapf(taskInfo.MapTaskInfo.FileName, string(content))
	saveIntermediateFile(taskInfo.TaskNum, kva)
	log.Printf("map task %v done \n", taskInfo.MapTaskInfo.FileName)
}

func handleReduceTask(reducef func(string, []string) string, taskInfo GetTaskReply) {

}

func saveIntermediateFile(n int64, kva []KeyValue) {
	log.Printf("get %d kva:", len(kva))
	resStr, err := json.Marshal(kva)
	if err != nil {
		panic(err)
	}
	intermediateFilename := fmt.Sprintf("mr-out-%d", n)
	ofile, _ := os.Create(intermediateFilename)
	defer ofile.Close()
	_, err = fmt.Fprint(ofile, resStr)
	if err != nil {
		panic(err)
	}
}

func readFile(filename string) []byte {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return content
}

func getJob() GetTaskReply {
	fmt.Println("worker request job--------------")
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Master.GetJob", &args, &reply)
	return reply
}

func finishJob(taskInfo GetTaskReply) FinishTaskReply {
	args := FinishTaskArgs{
		TaskType: taskInfo.TaskType,
		TaskNum:  taskInfo.TaskNum,
	}
	log.Printf("finish job : %v : %v", taskInfo.TaskType, taskInfo.TaskNum)

	reply := FinishTaskReply{}
	call("Master.FinishJob", &args, &reply)
	return reply
}

//
// example function to show how to make an RPC call to the master.`
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
