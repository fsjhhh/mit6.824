package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"
import "fmt"

const (
	MAP    = "MAP"
	REDUCE = "REDUCE"
)

type Task struct {
	Id           int       // 任务ID
	Type         string    // 任务类型
	MapInputFile string    // map的输入文件
	WorkerId     string    // worker的ID
	DeadLine     time.Time // 时间期限
}

// worker通过rpc申请任务
type ApplyTask struct {
	WorkerId     string // 申请任务worker的ID
	LastTaskId   int    // 上一个任务的ID
	LastTaskType string // 上一个任务的类型
}

// coordinator回复worker
type ReplyTask struct {
	TaskId       int    // 分配的任务ID
	TaskType     string // 分配的任务类型
	MapInputFile string // map操作的输入文件
	MapNum       int    // map task的数量，用于生成中间结果文件
	ReduceNum    int    // reduce task的数量，用于生成中间结果文件
}

// 临时map输出文件名
func tmpMapOutFile(workerId string, mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-%d-%d", workerId, mapIndex, reduceIndex)
}

// 最终map输出文件名
func finalMapOutFile(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

// 临时reduce输出文件名
func tmpReduceOutFile(workerId string, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-out-%d", workerId, reduceIndex)
}

// 最终reduce输出文件名
func finalReduceOutFile(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}

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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
