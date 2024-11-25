package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type TaskState int

const (
	IDLE TaskState = iota
	INPROGRESS
	COMPLETED
)

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	WAIT
	DONE
)

// type Tasks struct {
// 	mu           sync.Mutex
// 	splits       []string
// 	mapTasks     map[string]string // worker -> split
// 	intermediate []int             // map number
// 	reduceTasks  map[string]string // worker -> filename
// }

// type Workers struct {
// 	mu           sync.Mutex
// 	workerSplits map[string]string // worker -> split
// }

// type Coordinator struct {
// 	tasks        Tasks
// 	workers      Workers
// 	nReduce      int
// 	mapNumber    uint32
// 	reduceNumber uint32
// }

type Available bool

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    MapTasks
	reduceTasks ReduceTasks
	files       map[string]Available
	reduceIds   map[int]Available
}

type MapTasks struct {
	tasks          map[string]map[int]MapTask
	completedTasks int
	totalTasks     int
	id             int
}

type ReduceTasks struct {
	tasks          map[string]map[int]ReduceTask
	completedTasks int
	totalTasks     int
}

type MapTask struct {
	state TaskState
	file  string
	id    int
}

type ReduceTask struct {
	state   TaskState
	fileKey int // in an intermediate file: mr-mapId-<fileKey>
}

func (c *Coordinator) resetTask(worker string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tasks, ok := c.mapTasks.tasks[worker]
	if ok {
		for _, task := range tasks {
			if task.state == COMPLETED {
				c.mapTasks.completedTasks--
				c.files[task.file] = true
				// TODO notify reduce workers
			}
		}
		delete(c.mapTasks.tasks, worker)
		return
	}

	reduceTasks, ok := c.reduceTasks.tasks[worker]
	if ok {
		for _, reduceTask := range reduceTasks {
			if reduceTask.state == INPROGRESS {
				c.resetReduceNumber(reduceTask.fileKey)
			}
		}
		delete(c.reduceTasks.tasks, worker)
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	tasks := c.mapTasks.tasks[args.WorkerId]
	task := tasks[args.TaskId]
	task.state = COMPLETED
	c.mapTasks.tasks[args.WorkerId][args.TaskId] = task
	c.mapTasks.completedTasks++
	return nil
}

func (c *Coordinator) findReduceNumber() int {
	for i, available := range c.reduceIds {
		if available {
			c.reduceIds[i] = false
			return i
		}
	}
	return -1
}

func (c *Coordinator) resetReduceNumber(i int) {
	c.reduceIds[i] = true
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *ReqTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.reduceTasks.completedTasks == c.reduceTasks.totalTasks {
		reply.Type = DONE
		return nil
	}

	if c.mapTasks.completedTasks < c.mapTasks.totalTasks {
		for file, available := range c.files {
			if available {
				reply.Type = MAP
				reply.MapTask = MapArgs{
					File:       file,
					Partitions: c.reduceTasks.totalTasks,
					WorkerId:   args.WorkerId,
					TaskId:     c.mapTasks.id,
				}
				c.files[file] = false

				if _, ok := c.mapTasks.tasks[args.WorkerId]; !ok {
					c.mapTasks.tasks[args.WorkerId] = make(map[int]MapTask)
				}

				c.mapTasks.tasks[args.WorkerId][c.mapTasks.id] = MapTask{
					state: INPROGRESS,
					file:  file,
					id:    c.mapTasks.id,
				}
				c.mapTasks.id++
				return nil
			}
		}
		reply.Type = WAIT
		return nil
	}

	if c.reduceTasks.completedTasks < c.reduceTasks.totalTasks {
		reduceNumber := c.findReduceNumber()
		if reduceNumber == -1 {
			reply.Type = WAIT
			return nil
		}
		fileKeys := make([]string, c.mapTasks.totalTasks)
		for workerId := range c.mapTasks.tasks {
			for i, task := range c.mapTasks.tasks[workerId] {
				id := task.id
				fileKeys[i] = strconv.Itoa(id)
			}
		}
		reply.Type = REDUCE
		reply.ReduceTask = ReduceArgs{
			Partitions:   c.reduceTasks.totalTasks,
			WorkerIds:    fileKeys,
			ReduceNumber: reduceNumber,
			WorkerId:     args.WorkerId,
		}

		if _, ok := c.reduceTasks.tasks[args.WorkerId]; !ok {
			c.reduceTasks.tasks[args.WorkerId] = make(map[int]ReduceTask)
		}

		c.reduceTasks.tasks[args.WorkerId][reduceNumber] = ReduceTask{
			state:   INPROGRESS,
			fileKey: reduceNumber,
		}
		return nil
	}

	log.Println("RequestTask: Don't think this should happen")
	reply.Type = WAIT
	return nil
}

func (c *Coordinator) FinishReduce(args *ReduceFinishedArgs, reply *Empty) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	task := c.reduceTasks.tasks[args.WorkerId][args.ReduceNumber]
	task.state = COMPLETED
	c.reduceTasks.tasks[args.WorkerId][args.ReduceNumber] = task
	c.reduceTasks.completedTasks++
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *Empty) error {
	fmt.Println("Received worker")
	go c.pingWorker(args.Sockname, args.WorkerId)
	return nil
}

func (c *Coordinator) pingWorker(sockname string, workerId string) {
	for {
		ok := callWorker(sockname, "WorkerRPC.Ping", &Empty{}, &Empty{})
		if !ok {
			log.Println("failed to ping worker")
			c.resetTask(workerId)
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reduceTasks.completedTasks == c.reduceTasks.totalTasks
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	mapFiles := make(map[string]Available)
	for _, file := range files {
		mapFiles[file] = true
	}
	reduceIds := make(map[int]Available)
	for i := 0; i < nReduce; i++ {
		reduceIds[i] = true
	}
	c := Coordinator{
		mapTasks: MapTasks{
			tasks:          make(map[string]map[int]MapTask),
			completedTasks: 0,
			totalTasks:     len(files),
		},
		reduceTasks: ReduceTasks{
			tasks:          make(map[string]map[int]ReduceTask),
			completedTasks: 0,
			totalTasks:     nReduce,
		},
		files:     mapFiles,
		reduceIds: reduceIds,
	}

	c.server()
	return &c
}
