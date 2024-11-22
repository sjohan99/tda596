package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

func (c *WorkerRPC) ExampleWorker(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *WorkerRPC) Ping(args *Empty, reply *Empty) error {
	return nil
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func initWorker() {
	workerRPC := new(WorkerRPC)
	sockname := workerRPC.server()

	args := WorkerAddressArgs{sockname}
	reply := WorkerAddressReply{}

	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if !ok {
		log.Fatalf("failed to do task")
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	initWorker()

	args := WorkerArgs{}
	reply := WorkerReply{}

	for {
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			log.Fatalf("Coordinator.RequestTask failed")
		}
		if reply.Split == "" {
			fmt.Println("No available task, sleeping a while")
			time.Sleep(3 * time.Second)
		} else {
			break
		}
	}

	// TODO handle reply.Input being empty (no available task)
	fmt.Println(reply.Split)

	filename := reply.Split

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	fmt.Println("Map task", reply.MapNumber, "Reduce task", reply.ReduceNumber)

	kvs := mapf(filename, string(content))
	encodeFile(&reply, kvs)

}

func encodeFile(rep *WorkerReply, kvs []KeyValue) string {
	intermediateFilename := "mr-" + strconv.Itoa(int(rep.MapNumber))
	intermediateFile, err := os.Create(intermediateFilename)
	if err != nil {
		log.Fatalf("cannot create %v", intermediateFilename)
	}
	enc := json.NewEncoder(intermediateFile)
	for _, kv := range kvs {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}
	return intermediateFilename
}

func decodeFile(rep *WorkerReply) []KeyValue {
	intermediateFilename := "mr-" + strconv.Itoa(int(rep.MapNumber))
	intermediateFile, err := os.Create(intermediateFilename)
	if err != nil {
		log.Fatalf("cannot create %v", intermediateFilename)
	}
	dec := json.NewDecoder(intermediateFile)
	kvs := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

func (w *WorkerRPC) server() (sockname string) {
	rpc.Register(w)
	rpc.HandleHTTP()
	sockname = workerSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	return sockname
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
