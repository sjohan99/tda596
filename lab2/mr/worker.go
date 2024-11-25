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
	"sort"
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

	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	for {
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			log.Fatalf("Coordinator.RequestTask failed")
		}
		switch reply.Type {
		case "wait":
			fmt.Println("No available task, sleeping a while")
			time.Sleep(3 * time.Second)

		case "map":
			workerDoesMapping(reply, mapf)
		case "reduce":
			workerDoesReduce(reply, reducef)
		}
	}

	// TODO handle reply.Input being empty (no available task)

}

func workerDoesReduce(reply RequestTaskReply, reducef func(string, []string) string) {
	tempfile, err := ioutil.TempFile("", "mr-out-temp-*")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}

	intermediate := []KeyValue{}
	for _, mapNumber := range reply.FileNumbers {
		filename := fmt.Sprintf("mr-%d-%d", mapNumber, reply.ReduceNumber)
		intermediate = append(intermediate, decodeFile(filename)...)
	}
	sort.Sort(ByKey(intermediate))

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
		fmt.Fprintf(tempfile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempfile.Close()
	oname := fmt.Sprintf("mr-out-%d", reply.ReduceNumber)
	err = os.Rename(tempfile.Name(), oname)
	if err != nil {
		log.Fatalf("cannot rename %v to %v", tempfile.Name(), oname)
	}

	args := ReduceFinishedArgs{reply.ReduceNumber}
	reduceFinishedReply := Empty{}
	ok := call("Coordinator.FinishReduce", &args, &reduceFinishedReply)
	if !ok {
		log.Fatalf("Coordinator.FinishReduce failed")
	}
}

func workerDoesMapping(reply RequestTaskReply, mapf func(string, string) []KeyValue) {
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

	kvs := mapf(filename, string(content))
	part := len(kvs) / reply.R // TODO rename
	for i := 0; i < reply.R; i++ {
		s := kvs[i*part : (i+1)*part]
		encodeFile(&reply, s, i)
	}

	args := MapFinishedArgs{int(reply.MapNumber), reply.Split}
	mapFinishedReply := Empty{}
	ok := call("Coordinator.FinishMap", &args, &mapFinishedReply)
	if !ok {
		log.Fatalf("Coordinator.FinishMap failed")
	}
}

func encodeFile(rep *RequestTaskReply, kvs []KeyValue, i int) string {
	intermediateFilename := fmt.Sprintf("mr-%d-%d", rep.MapNumber, i)
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

func decodeFile(filename string) []KeyValue {
	intermediateFile, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer intermediateFile.Close()
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
