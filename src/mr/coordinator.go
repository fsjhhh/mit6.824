package mr

import (
	"log"
	"math"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"

type Coordinator struct {
	// Your definitions here.
	lock           sync.Mutex      // 互斥锁，避免冲突
	stage          string          // 当前任务: MAP / REDUCE
	nMap           int             // Map任务数量
	nReduce        int             // Reduce任务数量
	tasks          map[string]Task // 正在运行的任务
	availableTasks chan Task       // 未分配的任务
}

// 处理来自worker的请求
func (c *Coordinator) ApplyTaskForWorker(args *ApplyTask, reply *ReplyTask) error {
	// 记录 worker 的上一个任务已经完成
	if args.LastTaskType != "" {
		c.lock.Lock()
		lastTaskID := GetTaskID(args.LastTaskType, args.LastTaskId)
		if task, exists := c.tasks[lastTaskID]; exists && task.WorkerId == args.WorkerId {
			log.Printf(
				"Mark %s task %d as finished on worker %s\n",
				task.Type, task.Id, task.WorkerId)
			if args.LastTaskType == MAP {
				for ri := 0; ri < c.nReduce; ri++ {
					err := os.Rename(
						tmpMapOutFile(args.WorkerId, args.LastTaskId, ri),
						finalMapOutFile(args.LastTaskId, ri))
					if err != nil {
						log.Fatal(
							fmt.Sprintf("Failed to mark map output file %s as final: %v",
								tmpMapOutFile(args.WorkerId, args.LastTaskId, ri), err))
					}
				}
			} else if args.LastTaskType == REDUCE {
				err := os.Rename(
					tmpReduceOutFile(args.WorkerId, args.LastTaskId),
					finalReduceOutFile(args.LastTaskId))
				if err != nil {
					log.Fatal(
						fmt.Sprintf("Failed to mark reduce output file %s as final: %v",
							tmpReduceOutFile(args.WorkerId, args.LastTaskId), err))
				}
			}
			delete(c.tasks, lastTaskID)

			// 当前阶段所有任务完成，进入下一阶段
			if len(c.tasks) == 0 {
				c.transit()
			}
		}
		c.lock.Unlock()
	}

	// 获取一个可用的 task 并返回
	task, ok := <-c.availableTasks
	if !ok {
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	log.Printf("Assigning task %s to worker %s\n", task.Type, task.WorkerId)
	task.WorkerId = args.WorkerId
	task.DeadLine = time.Now().Add(10 * time.Second)
	c.tasks[GetTaskID(task.Type, task.Id)] = task
	reply.TaskId = task.Id
	reply.TaskType = task.Type
	reply.MapInputFile = task.MapInputFile
	reply.MapNum = c.nMap
	reply.ReduceNum = c.nReduce
	return nil
}

// 状态转换，MAP阶段完成则进入REDUCE阶段，REDUCE阶段完成则退出程序
func (c *Coordinator) transit() {
	if c.stage == MAP {
		log.Printf("ALL MAP tasks finished. Transit to REDUCE stage\n")
		c.stage = REDUCE

		// 生成 REDUCE 任务
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Id:   i,
				Type: REDUCE,
			}
			c.tasks[GetTaskID(task.Type, task.Id)] = task
			c.availableTasks <- task
		}
	} else if c.stage == REDUCE {
		log.Printf("ALL REDUCE tasks finished. Prepare to exit\n")
		close(c.availableTasks)
		c.stage = ""
	}
}

// MAP映射，类似于hash
func GetTaskID(t string, id int) string {
	return fmt.Sprintf("%s-%d", t, id)
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.stage == ""
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:          MAP,
		nMap:           len(files),
		nReduce:        nReduce,
		tasks:          make(map[string]Task),
		availableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))+1),
	}
	log.Printf("nReduce %d files\n", nReduce)

	for i, file := range files {
		task := Task{
			Id:           i,
			Type:         MAP,
			MapInputFile: file,
		}
		c.tasks[GetTaskID(task.Type, task.Id)] = task
		c.availableTasks <- task
	}
	log.Printf("Coordinator Start\n")
	c.server()

	// 启动 Task 超时回收
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			c.lock.Lock()
			for _, task := range c.tasks {
				if task.WorkerId != "" && time.Now().After(task.DeadLine) {
					// 回收任务并重新分配
					log.Printf(
						"Found time-out %s task %d previously running on worker %s. Preparing to re-assign\n",
						task.Type, task.Id, task.WorkerId)
					task.WorkerId = ""
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()
	return &c
}
