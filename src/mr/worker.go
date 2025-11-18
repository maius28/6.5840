package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)

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

	for {
		taskArgs, err := doWork(mapf, reducef)
		if err != nil {
			fmt.Printf("do work error: %v \n", err)
			//todo
		}

		if taskArgs.TaskType == Wait {
			fmt.Println("no idle task, wait 2 second")
			time.Sleep(2 * time.Second)
			continue
		} else if taskArgs.TaskType == Exit {
			fmt.Println("get exit sign, exit process")
			return
		} else {
			err = doAck(taskArgs)
			if err != nil {
				fmt.Printf("%v \n", err)
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doWork(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) (TaskArgs, error) {
	args := TaskArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.RegisterTask", &args, &reply)
	if ok {
		//map
		switch reply.TaskType {
		case Map:
			doMap(&reply, mapf)
		case Reduce:
			doReduce(&reply, reducef)
		case Wait:

		case Exit:

		default:

		}

	}
	return TaskArgs{Id: reply.Id, TaskType: reply.TaskType}, nil
}

func doAck(args TaskArgs) error {
	reply := TaskReply{}

	ok := call("Coordinator.AckTask", &args, &reply)
	if !ok {
		return fmt.Errorf("call askTask err")
	}

	return nil

}

func doMap(reply *TaskReply, mapf func(string, string) []KeyValue) error {
	fmt.Printf("get a map task: %v \n", reply)
	filename := reply.FileName
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

	partitionMap := make(map[int][]KeyValue)

	for _, kv := range kva {
		partition := ihash(kv.Key) % reply.nReduce
		partitionMap[partition] = append(partitionMap[partition], kv)
	}

	for partition, kva := range partitionMap {
		filename := fmt.Sprintf("map-*-%d", partition)
		tempFile, err := os.CreateTemp("", filename)
		if err != nil {
			fmt.Printf("create map intermediate file error: %v\n", err)
			return err
		}

		enc := json.NewEncoder(tempFile)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Printf("error encode json %v\n", err)
				return err
			}
		}

		tempFile.Close()
	}
	fmt.Println("map task finished")
	return nil
}

func doReduce(reply *TaskReply, reducef func(string, []string) string) (TaskArgs, error) {

	return TaskArgs{Id: reply.Id, TaskType: reply.TaskType}, nil
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
