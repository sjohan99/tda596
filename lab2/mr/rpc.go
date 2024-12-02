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
)

type ReqTaskArgs struct {
	WorkerId WorkerId
}

type ReqTaskReply struct {
	Type       TaskType
	MapTask    MapArgs
	ReduceTask ReduceArgs
}

type MapArgs struct {
	File       string   // File to read from
	Partitions int      // How many partitions to split the file into
	WorkerId   WorkerId // File key to use for intermediate files
	TaskId     MapTaskId
}

type ReduceArgs struct {
	Partitions int
	MapIds     []MapTaskId  // mr-<MapId>-<0..Partitions>
	ReduceId   ReduceTaskId // for output file -> mr-out-ReduceId
	WorkerId   WorkerId
}

type ReduceFinishedArgs struct {
	WorkerId WorkerId
	TaskId   ReduceTaskId
}

type RegisterWorkerArgs struct {
	Sockname string
	WorkerId WorkerId
}

type MapFinishedArgs struct {
	WorkerId string
	TaskId   MapTaskId
}

type Empty struct{}

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
