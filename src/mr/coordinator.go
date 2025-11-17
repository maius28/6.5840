package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	idleMapChan chan *Task //waiting worker to finish

	idleReduceTaskChan chan *Task

	TaskProcessingmMap map[int]*Task

	FinishedMapChan chan int

	TotalMapTask int

	AllMapFinished bool //all task count

	Finished bool // finished task count

	Timeout time.Duration
}

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Exit
)

type Task struct {
	Id        int
	FileName  string
	TaskType  TaskType
	ReduceId  int
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RegisterTask(args *TaskArgs, reply *TaskReply) error {
	//first get idle map task
	select {
	case task, ok := <-c.idleMapChan:
		if !ok {
			return fmt.Errorf("error: idleMap chan closed")
		}
		reply.TaskType = task.TaskType
		reply.FileName = task.FileName
		task.StartTime = time.Now()
		c.TaskProcessingmMap[task.Id] = task
		return nil
	default:
	}

	//if no idle map task, check all map task finished, if all finished, assgin reduce task

	//

}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

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
	ret := false

	// Your code here.

	return ret
}

// timeout checker
func (c *Coordinator) timeoutCheck() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		fmt.Println("timeout check tick")
		for workerId, task := range c.TaskProcessingmMap {
			if time.Since(task.StartTime) > c.Timeout {
				fmt.Printf("worker[%v] run timeout\n", workerId)
				delete(c.TaskProcessingmMap, workerId)
				c.idleMapChan <- task
			}
		}
	}
}

func (c *Coordinator) allMapFinishedCheck() {
	finishedCount := 0
	for finishedCount < c.TotalMapTask {
		<-c.FinishedMapChan
		finishedCount++
	}

	close(c.FinishedMapChan)
	fmt.Println("all map task finished")
	return
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	count := 0
	mapCount := len(files)
	// Your code here.
	c.idleMapChan = make(chan *Task, mapCount)
	c.idleReduceTaskChan = make(chan *Task, nReduce)
	c.FinishedMapChan = make(chan int)
	c.TotalMapTask = len(files)

	for i, file := range files {
		c.idleMapChan <- &Task{
			Id:       i + 1,
			FileName: file,
			TaskType: Map,
		}
		count++
	}

	for i := range nReduce {
		c.idleReduceTaskChan <- &Task{
			Id:       mapCount + i + 1,
			ReduceId: i,
		}
		count++
	}

	c.TaskProcessingmMap = make(map[int]*Task, count)
	c.Timeout = time.Second * 5 //if 5s worker not ack, assume the task failed

	c.server()

	go c.allMapFinishedCheck()
	go c.timeoutCheck()

	return &c
}
