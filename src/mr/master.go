package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus int64

const (
	TaskStatusPrepare TaskStatus = iota + 1
	TaskStatusProcessing
	TaskStatusDone
)

type Task struct {
	FileName string
	Status   TaskStatus
}

//type ReduceTask struct {
//	FileName string
//Status TaskStatus
//}

type Tasks struct {
	PrepareTasks    map[int64]int64
	ProcessingTasks map[int64]int64
	DoneTasks       map[int64]int64
	TaskList        []Task
}

type Master struct {
	// Your definitions here.
	mu      sync.Mutex
	nReduce int
	nMap    int

	mapDispatchOver    bool
	reduceDispatchOver bool

	mapTasks    Tasks
	reduceTasks Tasks
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
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.TaskType == TaskTypeMap {
		if args.TaskNum < int64(len(m.mapTasks.TaskList)) {
			m.mapTasks.TaskList[args.TaskNum].Status = TaskStatusDone // todo lock
		}
		delete(m.mapTasks.ProcessingTasks, args.TaskNum)
		m.mapTasks.DoneTasks[args.TaskNum] = 1
	} else if args.TaskType == TaskTypeReduce {
		if args.TaskNum < int64(len(m.reduceTasks.TaskList)) {
			m.reduceTasks.TaskList[args.TaskNum].Status = TaskStatusDone // todo lock
		}
		delete(m.reduceTasks.ProcessingTasks, args.TaskNum)
		m.reduceTasks.DoneTasks[args.TaskNum] = 1
	}

	log.Printf("get finish job report: %v : %v", args.TaskType, args.TaskNum)
	m.showTaskStatus()
	return nil
}

func (m *Master) allMapTaskDone() bool {
	//for _, t := range m.mapTasks {
	//	if t.Status != TaskStatusDone {
	//		return false
	//	}
	//}
	return true
}

func (m *Master) showTaskStatus() {
	log.Printf("map task prepare:  %v\n", m.mapTasks.PrepareTasks)
	log.Printf("map task processing:  %v\n", m.mapTasks.ProcessingTasks)
	log.Printf("map task done:  %v\n", m.mapTasks.DoneTasks)
	log.Printf("reduce task prepare:  %v\n", m.reduceTasks.PrepareTasks)
	log.Printf("reduce task processing:  %v\n", m.reduceTasks.ProcessingTasks)
	log.Printf("reduce task done:  %v\n", m.reduceTasks.DoneTasks)
}

func (m *Master) GetJob(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.mapTasks.PrepareTasks) != 0 { // have task not process
		for i, task := range m.mapTasks.TaskList { // todo optimize loop
			if task.Status == TaskStatusPrepare {
				mapTask := &MapTaskInfo{
					FileName: task.FileName,
					NReduce:  m.nReduce,
				}
				reply.TaskType = TaskTypeMap
				reply.MapTaskInfo = mapTask
				reply.TaskNum = int64(i)
				m.mapTasks.TaskList[i].Status = TaskStatusProcessing
				log.Printf("dispatched one map task : %d \n", i)

				delete(m.mapTasks.PrepareTasks, int64(i))
				m.mapTasks.ProcessingTasks[int64(i)] = 1
				return nil
			}
		}
	} else if len(m.mapTasks.PrepareTasks) == 0 && !m.mapDispatchOver { // all dispatched but not finish yet
		m.mapDispatchOver = true
		reply.TaskType = TaskTypeMapDispatchedOver
		return nil
	}

	if len(m.reduceTasks.PrepareTasks) != 0 { // have task not process
		for i, task := range m.reduceTasks.TaskList { // todo optimize loop
			if task.Status == TaskStatusPrepare {
				reduceTask := &ReduceTaskInfo{
					NMap: m.nMap,
				}
				reply.TaskType = TaskTypeReduce
				reply.ReduceTaskInfo = reduceTask
				reply.TaskNum = int64(i)
				m.reduceTasks.TaskList[i].Status = TaskStatusProcessing

				log.Printf("dispatched one reduce task : %d \n", i)

				delete(m.reduceTasks.PrepareTasks, int64(i))
				m.reduceTasks.ProcessingTasks[int64(i)] = 1
				m.showTaskStatus()
				return nil
			}
		}

	} else if len(m.reduceTasks.PrepareTasks) == 0 && !m.reduceDispatchOver { // all dispatched but not finish yet
		m.reduceDispatchOver = true
		reply.TaskType = TaskTypeReduceDispatchedOver
		return nil
	}
	reply.TaskType = TaskTypeReduceDispatchedOver
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
	for _, task := range m.mapTasks.TaskList {
		if task.Status != TaskStatusDone {
			return false
		}
	}
	for _, task := range m.reduceTasks.TaskList {
		if task.Status != TaskStatusDone {
			return false
		}
	}
	return true
}

func (m *Master) initMapTasks(files []string) {
	log.Println("initiating map task....")
	m.nMap = len(files)
	m.mapTasks = Tasks{
		PrepareTasks:    make(map[int64]int64),
		ProcessingTasks: make(map[int64]int64),
		DoneTasks:       make(map[int64]int64),
		TaskList:        nil,
	}
	for i, f := range files {
		m.mapTasks.TaskList = append(m.mapTasks.TaskList, Task{
			FileName: f,
			Status:   TaskStatusPrepare,
		})
		m.mapTasks.PrepareTasks[int64(i)] = 1
		log.Println("add map task:", f)
	}
	log.Println("init map task done....")
}

func (m *Master) initReduceTasks(nReduce int) {
	log.Println("initiating reduce task....")
	m.reduceTasks = Tasks{
		PrepareTasks:    make(map[int64]int64),
		ProcessingTasks: make(map[int64]int64),
		DoneTasks:       make(map[int64]int64),
		TaskList:        nil,
	}
	for i := 0; i < nReduce; i++ {
		m.reduceTasks.TaskList = append(m.reduceTasks.TaskList, Task{
			Status: TaskStatusPrepare,
		})
		m.reduceTasks.PrepareTasks[int64(i)] = 1
		log.Println("add reduce task:", i)
	}
	log.Println("init reduce task done....")
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce mapTasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce: nReduce,
	}
	m.mapTasks.TaskList = make([]Task, 0)

	// Your code here.
	m.initMapTasks(files)
	m.initReduceTasks(nReduce)

	m.server()
	return &m
}
