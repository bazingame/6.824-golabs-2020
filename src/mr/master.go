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
	MapTaskStatusPrepare MapTaskStatus = iota + 1
	MapTaskStatusProcessing
	MapTaskStatusDone
)

type Master struct {
	// Your definitions here.
	mu          sync.Mutex
	filesStatus map[string]MapTaskStatus
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

func (m *Master) GetJob(args *GetJobArgs, reply *GetJobReply) error {
	fmt.Println("get req")
	for fName, s := range m.filesStatus {
		if s == MapTaskStatusPrepare {
			reply = &GetJobReply{
				FileName: fName,
			}
			s = MapTaskStatusProcessing
			fmt.Println("returned : ", fName)
			return nil
		}
	}
	fmt.Println("returned nil")
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
	for _, status := range m.filesStatus {
		if status != MapTaskStatusDone {
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
	m.filesStatus = make(map[string]MapTaskStatus)

	// Your code here.
	for _, f := range files {
		m.filesStatus[f] = MapTaskStatusPrepare
	}
	fmt.Println(m.filesStatus)

	m.server()
	return &m
}
