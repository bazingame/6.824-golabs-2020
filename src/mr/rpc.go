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
	TaskType int64
	TaskNum  int64
}

type FinishTaskReply struct {
}

type GetTaskArgs struct {
}

const (
	TaskTypeMap int64 = iota + 1
	TaskTypeReduce
)

type MapTaskInfo struct {
	Num      int64
	FileName string
}

type ReduceTaskInfo struct {
	Num      int64
	FileName string
}

type GetTaskReply struct {
	TaskType       int64
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
