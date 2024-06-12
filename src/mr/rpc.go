package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// worker请求master分配任务的reply标志，worker以此来判断需要进行的操作
type ReqTaskReplyFlag int

// 枚举worker请求任务的reply标志
const (
	TaskGetted    ReqTaskReplyFlag = iota // master给worker分配任务成功
	WaitPlz                               // 当前阶段暂时没有尚未分配的任务，worker的本次请求获取不到任务
	FinishAndExit                         // mapreduce工作已全部完成，worker准备退出
)

// RPC请求master分配任务时的传入参数——事实上什么都不用传入，因为只是worker获取一个任务。判断分配什么类型的任务由Master根据执行阶段决定
type TaskArgs struct{}

// RPC请求master分配任务时的返回参数——即返回一个Master分配的任务
type TaskReply struct {
	Answer ReqTaskReplyFlag
	Task   Task
}

type FinArgs struct {
	TaskId int
}

type FinReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
