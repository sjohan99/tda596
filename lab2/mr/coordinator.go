package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// type TaskState int

// const (
// 	UNPROCCESSED TaskState = iota
// 	MAPPING      TaskState = iota
// 	REDUCING     TaskState = iota
// 	PROCCESSED   TaskState = iota
// )

type Tasks struct {
	mu           sync.Mutex
	splits       []string
	mapTasks     map[string]string // worker -> split
	intermediate []string          // filename
	reduceTasks  map[string]string // worker -> filename
}

type Workers struct {
	mu           sync.Mutex
	workerSplits map[string]string // worker -> split
}

type Coordinator struct {
	tasks        Tasks
	workers      Workers
	nReduce      int
	mapNumber    uint32
	reduceNumber uint32
}

func (c *Coordinator) resetTask(worker string) {
	c.workers.mu.Lock()
	split, ok := c.workers.workerSplits[worker]
	if !ok {
		return
	}
	delete(c.workers.workerSplits, worker)
	c.workers.mu.Unlock()

	// No split was assigned to this worker
	if split == "" {
		return
	}

	c.tasks.mu.Lock()
	defer c.tasks.mu.Unlock()
	delete(c.tasks.mapTasks, worker)
	delete(c.tasks.reduceTasks, worker)
	for _, existingSplit := range c.tasks.splits {
		if existingSplit == split {
			return
		}
	}
	c.tasks.splits = append(c.tasks.splits, split)
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) FinishMap(args *MapFinishedArgs, reply *Empty) error {
	c.tasks.mu.Lock()
	defer c.tasks.mu.Unlock()
	delete(c.tasks.mapTasks, args.Split)
	c.tasks.intermediate = append(c.tasks.intermediate, args.Filename)
	return nil
}

func (c *Coordinator) RequestTask(args *WorkerArgs, reply *WorkerReply) error {
	c.tasks.mu.Lock()
	defer c.tasks.mu.Unlock()
	splits := c.tasks.splits
	if len(splits) > 0 {
		reply.Split = splits[len(splits)-1]
		c.tasks.splits = splits[:len(splits)-1]
		reply.MapNumber = atomic.AddUint32(&c.mapNumber, 1)
		reply.R = c.nReduce
		return nil
	}
	reply.Split = ""
	return nil
}

func (c *Coordinator) RegisterWorker(args *WorkerAddressArgs, reply *WorkerAddressReply) error {
	fmt.Println("Received worker")
	c.workers.mu.Lock()
	c.workers.workerSplits[args.Sockname] = ""
	reply.Success = true
	c.workers.mu.Unlock()
	go c.pingWorker(args.Sockname)
	return nil
}

func (c *Coordinator) pingWorker(sockname string) {
	for {
		ok := callWorker(sockname, "WorkerRPC.Ping", &Empty{}, &Empty{})
		if !ok {
			log.Println("failed to ping worker")
			c.resetTask(sockname)
			break
		}
		time.Sleep(10 * time.Second)
	}
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

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		tasks: Tasks{
			splits:      files,
			mapTasks:    make(map[string]string),
			reduceTasks: make(map[string]string),
		},
		workers: Workers{
			workerSplits: make(map[string]string),
		},
		nReduce: nReduce,
	}

	c.server()
	return &c
}
