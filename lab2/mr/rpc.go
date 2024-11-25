package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

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

type RequestTaskArgs struct {
	WorkerId string
}

type RequestTaskReply struct {
	Type         string // "map" or "reduce" or "wait" or "done"
	Split        string
	R            int
	MapNumber    uint32
	FileNumbers  []int
	ReduceNumber int
}

type ReqTaskReply struct {
	Type       TaskType
	MapTask    MapArgs
	ReduceTask ReduceArgs
}

type MapArgs struct {
	File       string // File to read from
	Partitions int    // How many partitions to split the file into
	WorkerId   string // File key to use for intermediate files
	TaskId     int
}

type ReduceArgs struct {
	Partitions   int
	WorkerIds    []string // mr-<WorkerId>-<0..Partitions>
	ReduceNumber int      // for output file -> mr-out-ReduceNumber
	WorkerId     string
}

type ReduceFinishedArgs struct {
	WorkerId     string
	ReduceNumber int
}

type RegisterWorkerArgs struct {
	Sockname string
	WorkerId string
}

type MapFinishedArgs struct {
	WorkerId string
	TaskId   int
}

type Empty struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-coordinator-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func workerSock() (string, string) {
	id := strconv.Itoa(os.Getpid())
	s := "/var/tmp/5840-mr-worker-" + id
	return s, id
}

type WorkerRPC struct {
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

func callWorker(sockname, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("Could not dial worker:", err)
		return false
	}
	defer c.Close()

	call := c.Go(rpcname, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-time.After(10 * time.Second):
		log.Println("timeout")
		return false
	case resp := <-call.Done:
		if resp != nil && resp.Error != nil {
			log.Println(resp.Error)
			return false
		}
	}

	return true

	// err = c.Call(rpcname, args, reply)
	// if err == nil {
	// 	return true
	// }

	// fmt.Println(err)
	// return false
}
