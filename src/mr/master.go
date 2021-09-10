package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type MapTaskStatus int64

const (
	TaskStatusPrepare MapTaskStatus = iota + 1
	TaskStatusProcessing
	TaskStatusDone
)

type MapTask struct {
	FileName string
	Status   MapTaskStatus
}

type ReduceTask struct {
	FileName string
	Status   MapTaskStatus
}

type Master struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []MapTask
	reduceTasks []ReduceTask
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) FinishJob(args *FinishTaskArgs, reply *FinishTaskReply) error {
	if args.TaskType == TaskTypeMap {
		if args.TaskNum < int64(len(m.mapTasks)) {
			m.mapTasks[args.TaskNum].Status = TaskStatusDone // todo lock
		}
	} else if args.TaskType == TaskTypeReduce {
		if args.TaskNum < int64(len(m.reduceTasks)) {
			m.reduceTasks[args.TaskNum].Status = TaskStatusDone // todo lock
		}
	}



	log.Printf("get finish job report: %v : %v", args.TaskType, args.TaskNum)
	m.showTaskStatus()
	return nil
}

func (m *Master) showTaskStatus() {
	for _, j := range m.mapTasks {
		log.Printf("%v status is: %v\n", j.FileName, j.Status)
	}
	for _, j := range m.reduceTasks {
		log.Printf("%v status is: %v\n", j.FileName, j.Status)
	}
}

func (m *Master) GetJob(args *GetTaskArgs, reply *GetTaskReply) error {
	fmt.Println("get job called ----------------")

	for i, task := range m.mapTasks {
		if task.Status == TaskStatusPrepare {
			mapTask := &MapTaskInfo{
				FileName: task.FileName,
			}
			reply.TaskType = TaskTypeMap
			reply.MapTaskInfo = mapTask
			reply.TaskNum = int64(i)
			task.Status = TaskStatusProcessing

			fmt.Println(reply)

			log.Printf("dispatched one map task : %d \n", i)
			return nil
		}
	}

	log.Printf("----all map task done---- \n")

	for i, task := range m.reduceTasks {
		if task.Status == TaskStatusPrepare {
			reduceTask := &ReduceTaskInfo{
				Num:      int64(i),
				FileName: task.FileName,
			}
			reply.TaskType = TaskTypeReduce
			reply.ReduceTaskInfo = reduceTask
			task.Status = TaskStatusProcessing
			log.Printf("dispatched one reduce task : %d \n", i)
			return nil
		}
	}
	log.Printf("----all reuce task done---- \n")

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	for _, task := range m.mapTasks {
		if task.Status != TaskStatusDone {
			return false
		}
	}
	for _, task := range m.reduceTasks {
		if task.Status != TaskStatusDone {
			return false
		}
	}
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce mapTasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mapTasks = make([]MapTask, 0)

	// Your code here.
	log.Println("initiating task....")
	for _, f := range files {
		m.mapTasks = append(m.mapTasks, MapTask{
			FileName: f,
			Status:   TaskStatusPrepare,
		})
		log.Println("add task:", f)
	}
	log.Println("init done....")

	m.server()
	return &m
}
