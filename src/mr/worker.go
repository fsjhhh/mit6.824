package mr

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// 用于sort
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	// 单机下使用pid作为worker的id
	id := strconv.Itoa(os.Getpid())
	log.Printf("Worker %s starting", id)

	var lastTaskType string
	var lastTaskId int
	for {
		args := ApplyTask{
			WorkerId:     id,
			LastTaskType: lastTaskType,
			LastTaskId:   lastTaskId,
		}
		reply := ReplyTask{}
		call("Coordinator.ApplyTaskForWorker", args, &reply)

		if reply.TaskType == "" {
			// 作业完成，退出循环
			log.Printf("All worker is finished")
			break
		}

		log.Printf("Worker get task id: %d, type: %s", reply.TaskId, reply.TaskType)
		if reply.TaskType == MAP {
			// 运行MAP
			runMap(mapf, reply, id)
		} else if reply.TaskType == REDUCE {
			// 运行REDUCE
			runReduce(reducef, reply, id)
		}
		lastTaskType = reply.TaskType
		lastTaskId = reply.TaskId

		log.Printf("Finish task id: %d, type: %s", reply.TaskId, reply.TaskType)
	}

	log.Printf("Worker %s finished\n", id)
}

func runMap(mapf func(string, string) []KeyValue, reply ReplyTask, id string) {
	// 读取数据
	file, err := os.Open(reply.MapInputFile)
	if err != nil {
		log.Fatal(fmt.Sprintf("Failed to open map input file %s: %v", reply.MapInputFile, err))
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatal(fmt.Sprintf("Failed to read map input file %s: %v", reply.MapInputFile, err))
	}
	// 将数据输入 MAP 函数
	data := mapf(reply.MapInputFile, string(content))
	// 按 key 的 hash 值进行分片
	hashData := make(map[int][]KeyValue)
	for _, kv := range data {
		ha := ihash(kv.Key) % reply.ReduceNum
		hashData[ha] = append(hashData[ha], kv)
	}
	// 将结果写入文件
	for i := 0; i < reply.ReduceNum; i++ {
		ofile, _ := os.Create(tmpMapOutFile(id, reply.TaskId, i))
		for _, kv := range hashData[i] {
			_, _ = fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
		}
		_ = ofile.Close()
	}
}

func runReduce(reducef func(string, []string) string, reply ReplyTask, id string) {
	var lines []string
	for i := 0; i < reply.MapNum; i++ {
		inputFile := finalMapOutFile(i, reply.TaskId)
		log.Printf("FileName: %s", inputFile)
		file, err := os.Open(inputFile)
		if err != nil {
			log.Fatal(fmt.Sprintf("Failed to open output file %s: %v", inputFile, err))
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatal(fmt.Sprintf("Failed to read output file %s: %v", inputFile, err))
		}
		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	var data []KeyValue
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		kv := strings.Split(line, "\t")
		data = append(data, KeyValue{kv[0], kv[1]})
	}
	sort.Sort(ByKey(data))

	ofile, _ := os.Create(tmpReduceOutFile(id, reply.TaskId))
	i := 0
	for i < len(data) {
		j := i + 1
		for j < len(data) && data[j].Key == data[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, data[k].Value)
		}
		output := reducef(data[i].Key, values)

		_, _ = fmt.Fprintf(ofile, "%v %v\n", data[i].Key, output)

		i = j
	}
	_ = ofile.Close()
}

// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
