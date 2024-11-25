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

func initWorker() string {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	workerRPC := new(WorkerRPC)
	sockname, id := workerRPC.server()

	args := RegisterWorkerArgs{sockname, id}

	ok := call("Coordinator.RegisterWorker", &args, &Empty{})
	if !ok {
		log.Fatalf("failed to do task")
	}
	return id
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	id := initWorker()

	args := RequestTaskArgs{id}
	reply := ReqTaskReply{}

	for {
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			log.Fatalf("Coordinator.RequestTask failed")
		}
		switch reply.Type {
		case WAIT:
			fmt.Println("No available task, sleeping a while")
			time.Sleep(3 * time.Second)

		case MAP:
			workerDoesMapping(reply.MapTask, mapf)
		case REDUCE:
			workerDoesReduce(reply.ReduceTask, reducef)
		case DONE:
			// TODO notify coordinator that worker is done
			return
		}
	}
}

func workerDoesReduce(args ReduceArgs, reducef func(string, []string) string) {
	tempfile, err := ioutil.TempFile("", "mr-out-temp-*")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}

	intermediate := []KeyValue{}
	for _, workerId := range args.WorkerIds {
		filename := fmt.Sprintf("mr-%v-%d", workerId, args.ReduceNumber)
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
	oname := fmt.Sprintf("mr-out-%d", args.ReduceNumber)
	err = os.Rename(tempfile.Name(), oname)
	if err != nil {
		log.Fatalf("cannot rename %v to %v", tempfile.Name(), oname)
	}

	finishedArgs := ReduceFinishedArgs{args.WorkerId, args.ReduceNumber}
	reduceFinishedReply := Empty{}
	ok := call("Coordinator.FinishReduce", &finishedArgs, &reduceFinishedReply)
	if !ok {
		log.Fatalf("Coordinator.FinishReduce failed")
	}
}

func workerDoesMapping(args MapArgs, mapf func(string, string) []KeyValue) {
	filename := args.File
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
	part := len(kvs) / args.Partitions // TODO rename
	for i := 0; i < args.Partitions; i++ {
		s := kvs[i*part : (i+1)*part]
		encodeFile(strconv.Itoa(args.TaskId), s, i)
	}

	finishedArgs := MapFinishedArgs{args.WorkerId, args.TaskId}
	mapFinishedReply := Empty{}
	ok := call("Coordinator.FinishMap", &finishedArgs, &mapFinishedReply)
	if !ok {
		log.Fatalf("Coordinator.FinishMap failed")
	}
}

func encodeFile(mapId string, kvs []KeyValue, i int) string {
	intermediateFilename := fmt.Sprintf("mr-%v-%d", mapId, i)
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

func (w *WorkerRPC) server() (string, string) {
	rpc.Register(w)
	rpc.HandleHTTP()
	sockname, id := workerSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	return sockname, id
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
