package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	loop := true
	for loop {
		re := RequestTask()
		switch re.Answer {
		case TaskGetted:
			task := re.Task
			switch task.TaskType {
			case MapTask:
				fmt.Printf("A worker get a map task and taskId is %d\n", task.TaskId)
				PerformMapTask(mapf, &task)
				FinishTaskAndReport(task.TaskId)
			case ReduceTask:
				fmt.Printf("A worker get a reduce task and taskId is %d\n", task.TaskId)
				PerformReduceTask(reducef, &task)
				FinishTaskAndReport(task.TaskId)
			}
		case WaitPlz:
			time.Sleep(time.Second)
		case FinishAndExit:
			loop = false
		default:
			fmt.Println("request task error!")
		}
	}
}

// 向master请求RPC,获取任务
func RequestTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}

	ok := call("Master.AssignTask", &args, &reply)
	if !ok {
		fmt.Println("Call AssignTask failed!")
	}
	return reply
}

// worker完成任务后调用RPC告知master并请求修改任务状态
func FinishTaskAndReport(id int) {
	args := FinArgs{TaskId: id}
	reply := FinReply{}

	ok := call("Master.UpdateTaskState", &args, &reply)
	if !ok {
		fmt.Println("Call UpdateTaskState failed!")
	} else {
		fmt.Println("Call UpdateTaskState success!")
	}
}

// 执行map任务
// mapf和reducef是mrworker.go创建worker时传进来的
// task是向Master请求后返回的待处理的任务
func PerformMapTask(mapf func(string, string) []KeyValue, task *Task) {
	intermediate := []KeyValue{}
	for _, filename := range task.InputFile {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	// 将中间键值对写入中间tmp文件
	// 中间文件的合理命名方式是 `mr-X-Y` ，X是Map任务的序号，Y是ihash(key)%nreduce后的值，这样就建立了key与被分配给的reduce任务的映射关系
	// map任务。可以使用Go的`encoding/json` package去存储中间的键值对到文件以便reduce任务之后读取
	rn := task.ReducerNum
	for i := 0; i < rn; i++ {
		midFileName := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		midFile, _ := os.Create(midFileName)
		enc := json.NewEncoder(midFile)
		for _, kv := range intermediate {
			if ihash(kv.Key)%rn == i {
				enc.Encode(&kv)
			}
		}
		midFile.Close()
	}
}

// 执行reduce任务
func PerformReduceTask(reducef func(string, []string) string, task *Task) {
	//intermediate := []KeyValue{}
	// 对reduce的所有输入中间文件洗牌得到一个有序的kv切片
	intermediate := shuffle(task.InputFile)
	// 为了确保没有人在出现崩溃的情况下观察到部分写入的文件，MapReduce论文提到了使用临时文件并在完成写入后原子地重命名它的技巧
	// 使用 ioutil.TempFile 来创建一个临时文件以及 os.Rename 来原子性地重命名它
	dir, _ := os.Getwd()
	// CreateTemp 在目录dir中新建一个临时文件，打开文件进行读写，返回结果文件
	// pattern 这是用于生成临时文件的名称。该名称是通过在模式的末尾附加一个随机字符串来创建的
	tmpfile, err := os.CreateTemp(dir, "mr-out-tmpfile-")
	if err != nil {
		log.Fatal("failed to create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tmpfile.Close()

	// reudce的输出文件，命名格式为：mr-out-*，其中*通过task记录的ReducerKth获取
	oname := dir + "/mr-out-" + strconv.Itoa(task.ReducerKth)

	//test code
	// fmt.Println("begin rename tmpFile")
	// fmt.Printf("dir: %s\n", dir)
	// fmt.Printf("tmpFile: %s\n", tmpfile.Name())
	// fmt.Printf("oname: %s\n", oname)
	//test code

	if err := os.Rename(tmpfile.Name(), oname); err != nil {
		fmt.Println(err)
	}
}

// shuffle函数，将一个reduce任务的所有输入中间文件中的kv排序
func shuffle(files []string) []KeyValue {
	kva := []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	//sockname := "/var/tmp/824-mr-1000"
	//这里为什么 masterSock 不一致
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
