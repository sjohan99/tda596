package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"time"
)

type ByKey []KeyValue

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
//
// Request tasks from the coordinator repeatedly until all tasks are done.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	id := strconv.Itoa(os.Getpid())

	for {
		args := ReqTaskArgs{id}
		reply := ReqTaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			log.Fatalf("Coordinator.RequestTask failed")
		}
		switch reply.Type {
		case WAIT:
			time.Sleep(time.Second)
		case MAP:
			workerMap(reply.MapTask, mapf)
		case REDUCE:
			workerReduce(reply.ReduceTask, reducef)
		case DONE:
			return
		}
	}
}

// Perform the reduce task and write the output file.
func workerReduce(args ReduceArgs, reducef func(string, []string) string) {
	tempfile, err := ioutil.TempFile("", "mr-out-temp-*")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}

	// read all intermediate files
	intermediate := []KeyValue{}
	for _, mapId := range args.MapIds {
		filename := fmt.Sprintf("mr-%v-%d", mapId, args.ReduceId)
		intermediate = append(intermediate, decodeFile(filename)...)
	}
	sort.Sort(ByKey(intermediate))

	// call reduce function on each unique key in intermediate and write to output
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

	// rename temp file to output file
	oname := fmt.Sprintf("mr-out-%d", args.ReduceId)
	err = os.Rename(tempfile.Name(), oname)
	if err != nil {
		log.Fatalf("cannot rename %v to %v", tempfile.Name(), oname)
	}

	// notify the coordinator that the reduce task is done
	finishedArgs := ReduceFinishedArgs{args.WorkerId, args.ReduceId}
	reduceFinishedReply := Empty{}
	ok := call("Coordinator.FinishReduce", &finishedArgs, &reduceFinishedReply)
	if !ok {
		log.Fatalf("Coordinator.FinishReduce failed")
	}
}

// Perform the map task and write the intermediate files.
func workerMap(args MapArgs, mapf func(string, string) []KeyValue) {
	content := readFile(args.File)
	kvs := mapf(args.File, content)

	// partition the key-values into buckets
	buckets := make([][]KeyValue, args.Partitions)
	for _, kv := range kvs {
		bucket := ihash(kv.Key) % args.Partitions
		buckets[bucket] = append(buckets[bucket], kv)
	}

	// write the buckets to intermediate files
	for reduceId := 0; reduceId < args.Partitions; reduceId++ {
		mapId := args.TaskId
		bucket := buckets[reduceId]
		encodeFile(mapId, reduceId, bucket)
	}

	// notify the coordinator that the map task is done
	finishedArgs := MapFinishedArgs{args.WorkerId, args.TaskId}
	mapFinishedReply := Empty{}
	ok := call("Coordinator.FinishMap", &finishedArgs, &mapFinishedReply)
	if !ok {
		log.Fatalf("Coordinator.FinishMap failed")
	}
}

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

// Encode each key-value pair as json objects into a file named mr-<MapId>-<ReduceId>
func encodeFile(mapId MapTaskId, reduceId ReduceTaskId, kvs []KeyValue) string {
	intermediateFilename := fmt.Sprintf("mr-%d-%d", mapId, reduceId)
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

// Decode json objects from a file and return a slice of KeyValue
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
