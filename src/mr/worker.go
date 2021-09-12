package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
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
	var mapWg sync.WaitGroup
	var reduceWg sync.WaitGroup
	for {
		taskInfo := getJob()
		log.Printf("get one job from master %v ...... \n", taskInfo)
		if taskInfo.TaskType == TaskTypeMap {
			mapWg.Add(1)
			go func() {
				defer mapWg.Done()
				handleMapTask(mapf, taskInfo)
			}()
		} else if taskInfo.TaskType == TaskTypeMapDispatchedOver {
			break
		}
		//time.Sleep(1 * time.Second)
	}
	log.Printf("workers knows that all MAP task DISPATCHER over!!!")
	mapWg.Wait()
	log.Printf("workers knows that all MAP task PROCESS over!!!")

	for {
		taskInfo := getJob()
		log.Printf("get one job from master %v ...... \n", taskInfo)
		if taskInfo.TaskType == TaskTypeReduce {
			reduceWg.Add(1)
			go func() {
				defer reduceWg.Done()
				handleReduceTask(reducef, taskInfo)
			}()
		} else if taskInfo.TaskType == TaskTypeReduceDispatchedOver {
			break
		}
	}
	log.Printf("workers knows that all REDUCE task DISPATCHER over!!!")
	reduceWg.Wait()
	log.Printf("workers knows that all REDUCE task PROCESS over!!!")

	return
}

func handleMapTask(mapf func(string, string) []KeyValue, taskInfo GetTaskReply) {
	log.Printf("handling map task %v ...... \n", taskInfo.MapTaskInfo.FileName)
	content := readFile(taskInfo.MapTaskInfo.FileName)
	kva := mapf(taskInfo.MapTaskInfo.FileName, string(content))
	saveIntermediateFile(taskInfo.TaskNum, taskInfo.MapTaskInfo.NReduce, kva)
	log.Printf("map task %v done \n", taskInfo.MapTaskInfo.FileName)
	finishJob(taskInfo)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func handleReduceTask(reducef func(string, []string) string, taskInfo GetTaskReply) {
	log.Printf("handling reduce task %v ...... \n", taskInfo.TaskNum)
	intermediate := make([]KeyValue, 0)

	for i := 0; i < taskInfo.ReduceTaskInfo.NMap; i++ {
		intermediateFilename := fmt.Sprintf("mr-%d-%d", i, taskInfo.TaskNum)
		file, err := os.Open(intermediateFilename)
		defer file.Close()
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFilename)
		}

		var kvs []KeyValue
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		intermediate = append(intermediate, kvs...)
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", taskInfo.TaskNum)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	log.Printf("reduce task %v done \n", taskInfo.TaskNum)
	finishJob(taskInfo)
}

func saveIntermediateFile(n int64, nReduce int, kva []KeyValue) {
	log.Printf("get %d kva:", len(kva))
	keyBuckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		bucketNum := ihash(kv.Key) % nReduce
		keyBuckets[bucketNum] = append(keyBuckets[bucketNum], kv)
	}

	for i := 0; i < nReduce; i++ {
		intermediateFilename := fmt.Sprintf("mr-%d-%d", n, i)
		ofile, _ := os.Create(intermediateFilename)
		defer ofile.Close()

		//resStr, err := json.Marshal(keyBuckets[i])

		enc := json.NewEncoder(ofile)
		for _, kv := range keyBuckets[i] {
			err := enc.Encode(&kv)
			if err != nil {
				panic(err)
			}
		}
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
	log.Printf("worker request job...")
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Master.GetJob", &args, &reply)
	log.Printf("get request job %v, %v, %v,%v", reply.TaskType.ToString(), reply.TaskNum, reply.MapTaskInfo, reply.ReduceTaskInfo)
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

	panic(err)
	return false
}
