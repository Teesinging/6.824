package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 定义全局互斥锁，worker访问Master时加锁。用于保证函数内并发安全
var mu sync.Mutex

type TaskType int

type TaskState int

type Phase int

const (
	MapTask TaskType = iota
	ReduceTask
)

const (
	Working TaskState = iota
	Waiting
	Fished
)

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

type Task struct {
	TaskId     int
	TaskType   TaskType
	TaskState  TaskState
	InputFile  []string
	ReducerNum int
	ReducerKth int
	StartTime  time.Time
}

type Master struct {
	// Your definitions here.
	CurrentPhase      Phase
	TaskIdForGen      int
	MapTaskChannel    chan *Task
	ReduceTaskChannel chan *Task
	TaskMap           map[int]*Task
	MapperNum         int
	ReducerNum        int
}

func (m *Master) GenerateTaskId() int {
	res := m.TaskIdForGen
	m.TaskIdForGen++
	return res
}

func (m *Master) MakeMapTask(files []string) {
	fmt.Println("begin make map tasks...")

	for _, file := range files {
		id := m.GenerateTaskId()
		input := []string{file}
		newTask := Task{
			TaskId:     id,
			TaskType:   MapTask,
			TaskState:  Waiting,
			InputFile:  input,
			ReducerNum: m.ReducerNum,
		}
		fmt.Printf("make map task %d success\n", id)
		//fmt.Println("task is")
		//fmt.Println(newTask)
		m.MapTaskChannel <- &newTask
	}

	// test code
	// close(m.MapTaskChannel)
	// go func() {
	// 	for task := range m.MapTaskChannel {
	// 		fmt.Printf("Task ID: %d, Task Name: %s\n", task.TaskId, task.InputFile)
	// 	}
	// }()
	// test code
}

func (m *Master) MakeReduceTask() {
	fmt.Println("begin make reduce tasks...")

	rn := m.ReducerNum
	dir, _ := os.Getwd()
	files, err := os.ReadDir(dir)
	if err != nil {
		fmt.Println(err)
	}
	for i := 0; i < rn; i++ {
		id := m.GenerateTaskId()
		input := []string{}
		for _, file := range files {
			if strings.HasPrefix(file.Name(), "mr-") && strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
				input = append(input, file.Name())
			}
		}

		reduceTask := Task{
			TaskId:     id,
			TaskType:   ReduceTask,
			TaskState:  Waiting,
			InputFile:  input,
			ReducerNum: m.ReducerNum,
			ReducerKth: i,
		}

		fmt.Printf("make a reduce task %d\n", id)
		m.ReduceTaskChannel <- &reduceTask
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 当worker完成任务时，会调用RPC通知master
// master得知后负责修改worker完成任务的状态,以便master检查该阶段任务是否全部已完成
func (m *Master) UpdateTaskState(args *FinArgs, reply *FinReply) error {
	mu.Lock()
	defer mu.Unlock()
	id := args.TaskId
	m.TaskMap[id].TaskState = Fished
	fmt.Printf("Task[%d] has been finished!\n", id)
	return nil
}

func (m *Master) AssignTask(args *TaskArgs, reply *TaskReply) error {
	mu.Lock()
	defer mu.Unlock()
	fmt.Println("Master get a RPC request from worker...")

	switch m.CurrentPhase {
	case MapPhase:
		if len(m.MapTaskChannel) > 0 {
			taskp := <-m.MapTaskChannel
			// 好像没有必要判断 waiting，因为生成的都是 waiting
			if taskp.TaskState == Waiting {
				reply.Answer = TaskGetted
				reply.Task = *taskp
				taskp.TaskState = Working
				taskp.StartTime = time.Now()
				m.TaskMap[(*taskp).TaskId] = taskp
				fmt.Printf("Task[%d] has been assigned.\n", taskp.TaskId)
			}
		} else {
			reply.Answer = WaitPlz
			if m.checkMapTaskDone() {
				m.toNextPhase()
			}
			return nil
		}
	case ReducePhase:
		if len(m.ReduceTaskChannel) > 0 {
			taskp := <-m.ReduceTaskChannel
			if taskp.TaskState == Waiting {
				reply.Answer = TaskGetted
				reply.Task = *taskp
				taskp.TaskState = Working
				taskp.StartTime = time.Now()
				m.TaskMap[(*taskp).TaskId] = taskp
				fmt.Printf("Task[%d] has been assigned.\n", taskp.TaskId)
			}
		} else {
			reply.Answer = WaitPlz
			if m.checkReduceTaskDone() {
				m.toNextPhase()
			}
			return nil
		}
	case AllDone:
		reply.Answer = FinishAndExit
		fmt.Println("All tasks have been finished")
	default:
		panic("Undefined Phase!")
	}
	return nil
}

// Master检查map阶段任务是否全部完成，若是则转入reduce阶段
func (m *Master) checkMapTaskDone() bool {
	var mapDoneNum = 0
	var mapUnDoneNum = 0
	for _, v := range m.TaskMap {
		if v.TaskType == MapTask {
			if v.TaskState == Fished {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		}
	}

	if mapDoneNum == m.MapperNum && mapUnDoneNum == 0 {
		return true
	}
	return false
}

// Master检查reduce阶段任务是否全部完成，若是则工作完成转入AllDone阶段，准备结束程序
func (m *Master) checkReduceTaskDone() bool {
	var reduceDoneNum = 0
	var reduceUnDoneNum = 0

	for _, v := range m.TaskMap {
		if v.TaskType == ReduceTask {
			if v.TaskState == Fished {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}

	//test code
	fmt.Println("checking Reduce Task Done?...")
	fmt.Printf("reduceDoneNum = %d\n", reduceDoneNum)
	fmt.Printf("reduceUnDoneNum = %d\n", reduceUnDoneNum)
	fmt.Printf("ReducerNum = %d\n", m.ReducerNum)
	//test code

	if reduceDoneNum == m.ReducerNum && reduceUnDoneNum == 0 {

		//test code
		fmt.Println("Reduce Task All Done! To next Phase: FinishAndExit")
		//test code

		return true
	}
	return false
}

// Master在判断当前阶段工作已完成后转入下一阶段
func (m *Master) toNextPhase() {
	switch m.CurrentPhase {
	case MapPhase:
		m.CurrentPhase = ReducePhase
		m.MakeReduceTask()
	case ReducePhase:
		m.CurrentPhase = AllDone
	}
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	fmt.Println("begin RPCserver...")
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
	fmt.Println("begin listen ")
	fmt.Println(sockname)
}

// master不能可靠地区分崩溃的工作线程、还活着但由于某种原因而停滞的工作线程和正在执行但速度太慢而无法使用的工作线程。
// 可以让master等待一段时间（如10s），然后放弃并将任务重新分配给其他worker，在此之后，master应该认为那个worker已经死亡
func (m *Master) CrashHandle() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if m.CurrentPhase == AllDone {
			mu.Unlock()
			break
		}

		for _, task := range m.TaskMap {
			if task.TaskState == Working && time.Since(task.StartTime) > time.Second*10 {
				fmt.Printf("Task[%d] is crashed!\n", task.TaskId)
				task.TaskState = Waiting
				switch task.TaskType {
				case MapTask:
					m.MapTaskChannel <- task
				case ReduceTask:
					m.ReduceTaskChannel <- task
				}
				delete(m.TaskMap, task.TaskId)
			}
		}
		mu.Unlock()
	}
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	mu.Lock()
	defer mu.Unlock()

	if m.CurrentPhase == AllDone {
		ret = true
	}

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	fmt.Println("begin make a master...")
	m := Master{
		CurrentPhase:      MapPhase,
		TaskIdForGen:      0,
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		TaskMap:           make(map[int]*Task, len(files)+nReduce),
		MapperNum:         len(files),
		ReducerNum:        nReduce,
	}

	fmt.Println("master make success!")
	//fmt.Println(m)
	m.MakeMapTask(files)

	// Your code here.

	m.server()
	go m.CrashHandle()
	return &m
}
