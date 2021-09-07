package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type MapTaskStatus int64

const (
	MapTaskStatusPrepare MapTaskStatus = iota + 1
	MapTaskStatusProcessing
	MapTaskStatusDone
)

type Task struct {
	FileName string
	Status   MapTaskStatus
}

type Master struct {
	// Your definitions here.
	mu    sync.Mutex
	tasks []Task
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

func (m *Master) GetJob(args *GetTaskArgs, reply *GetTaskReply) error {
	for i, task := range m.tasks {
		if task.Status == MapTaskStatusPrepare {
			reply.FileName = task.FileName
			reply.Num = int64(i)
			task.Status = MapTaskStatusProcessing
			return nil
		}
	}
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
	for _, task := range m.tasks {
		if task.Status != MapTaskStatusDone {
			return false
		}
	}
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.tasks = make([]Task, 0)

	// Your code here.
	for _, f := range files {
		m.tasks = append(m.tasks, Task{
			FileName: f,
			Status:   MapTaskStatusPrepare,
		})
	}
	log.Println("get task:", m.tasks)

	m.server()
	return &m
}
