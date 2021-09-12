package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type FinishTaskArgs struct {
	TaskType TaskType
	TaskNum  int64
}

type FinishTaskReply struct {
}

type GetTaskArgs struct {
}

type TaskType int64

func (t TaskType) ToString() string {
	switch t {
	case TaskTypeMap:
		return "Map"
	case TaskTypeReduce:
		return "Reduce"
	case TaskTypeMapDispatchedOver:
		return "MapDispatchedOver"
	case TaskTypeReduceDispatchedOver:
		return "ReduceDispatchedOver"

	}
	return "error type"
}

const (
	TaskTypeMap TaskType = iota + 1
	TaskTypeReduce
	TaskTypeMapDispatchedOver
	TaskTypeReduceDispatchedOver
)

type MapTaskInfo struct {
	FileName string
	NReduce  int
}

type ReduceTaskInfo struct {
	NMap int
}

type GetTaskReply struct {
	TaskType       TaskType
	TaskNum        int64
	MapTaskInfo    *MapTaskInfo
	ReduceTaskInfo *ReduceTaskInfo
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
