package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	args := TaskArgs{}
	reply := TaskReply{}

	for {
		fmt.Println("ask task")
		ok := call("Coordinator.RegisterTask", &args, &reply)
		var err error = nil
		if ok {
			//map
			switch reply.TaskType {
			case Map:
				err = doMap(&reply, mapf)
			case Reduce:
				err = doReduce(&reply, reducef)
			case Wait:
				fmt.Println("no idle task, wait 2 second")
				time.Sleep(2 * time.Second)
				continue
			case Exit:
				fmt.Println("get exit sign, exit process")
				return
			default:
				err = fmt.Errorf("non avaliable taskType %v", reply.TaskType)
			}

			if err == nil {
				err = doAck(TaskArgs{Id: reply.Id, TaskType: reply.TaskType})
				if err != nil {
					fmt.Printf("%v \n", err)
				}
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
		partition := ihash(kv.Key) % reply.NReduce
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

func doReduce(reply *TaskReply, reducef func(string, []string) string) error {
	partition := reply.ReduceId
	match := os.TempDir() + "map-*-" + strconv.Itoa(partition)
	filepaths, err := filepath.Glob(match)
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	if len(filepaths) == 0 {
		return fmt.Errorf("no match files: %v", match)
	}

	intermediate := []KeyValue{}

	for _, filepath := range filepaths {
		file, err := os.Open(filepath)
		if err != nil {
			return fmt.Errorf("%v", err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		err = os.Remove(filepath)
		if err != nil {
			fmt.Printf("remove file err: %v \n", err)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(partition)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	return nil
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
