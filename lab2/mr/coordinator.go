package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	INPROGRESS TaskState = iota
	COMPLETED
)

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	WAIT
	DONE
)

const (
	AVAILABLE   IsAvailable = true
	UNAVAILABLE IsAvailable = false
)

type IsAvailable bool

// Used to uniquely identify intermediate files and output files
// Intermediate files are named as "mr-<MapTaskId>-<ReduceTaskId>"
// Output files are named as "mr-out-<ReduceTaskId>"
type MapTaskId = int
type ReduceTaskId = int

// Used to uniquely identify workers
type WorkerId = string

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    MapTasks
	reduceTasks ReduceTasks
	files       map[string]IsAvailable
	reduceIds   map[ReduceTaskId]IsAvailable
}

type MapTasks struct {
	tasks          map[WorkerId]map[MapTaskId]MapTask
	completedTasks int
	totalTasks     int
	idCounter      MapTaskId // Used to assign unique ids to map tasks, increment by 1 for each new task
}

type ReduceTasks struct {
	tasks          map[WorkerId]map[ReduceTaskId]ReduceTask
	completedTasks int
	totalTasks     int
}

type MapTask struct {
	state TaskState
	file  string
}

type ReduceTask struct {
	state TaskState
}

// Request a map or reduce task from the Coordinator. If there are no idle tasks the
// worker will be asked to wait.
// If all tasks are completed, the worker will be notified that the job is done.
func (c *Coordinator) RequestTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer log.Printf("Status - Map tasks %d/%d | Reduce tasks %d/%d", c.mapTasks.completedTasks, c.mapTasks.totalTasks, c.reduceTasks.completedTasks, c.reduceTasks.totalTasks)

	switch {
	case c.reduceTasks.completedTasks == c.reduceTasks.totalTasks:
		reply.Type = DONE
	case c.mapTasks.completedTasks < c.mapTasks.totalTasks:
		success := c.tryCreateMapTask(reply, args)
		if !success { // No idle map tasks but need to wait for all map tasks to complete
			reply.Type = WAIT
		}
	case c.reduceTasks.completedTasks < c.reduceTasks.totalTasks:
		success := c.tryCreateReduceTask(reply, args)
		if !success { // No idle reduce tasks but need to wait for all reduce tasks to complete
			reply.Type = WAIT
		}
	}

	return nil
}

// Mark task as completed and increment map completedTasks counter
func (c *Coordinator) FinishMap(args *MapFinishedArgs, reply *Empty) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	task := c.mapTasks.tasks[args.WorkerId][args.TaskId]
	task.state = COMPLETED
	c.mapTasks.tasks[args.WorkerId][args.TaskId] = task
	c.mapTasks.completedTasks++
	return nil
}

// Mark task as completed and increment reduce completedTasks counter
func (c *Coordinator) FinishReduce(args *ReduceFinishedArgs, reply *Empty) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	task := c.reduceTasks.tasks[args.WorkerId][args.TaskId]
	task.state = COMPLETED
	c.reduceTasks.tasks[args.WorkerId][args.TaskId] = task
	c.reduceTasks.completedTasks++
	return nil
}

// Register worker by starting a goroutine to ping the worker every 10 seconds
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *Empty) error {
	go c.pingWorker(args.Sockname, args.WorkerId)
	return nil
}

// Ping worker every 10 seconds to check if it is still alive
// If worker is not alive, reset tasks assigned to the worker if necessary
func (c *Coordinator) pingWorker(sockname string, workerId WorkerId) {
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

// Resets tasks assigned to the worker
//
// Map tasks are always completely reset.
// Reduce tasks are reset only if they are in progress.
// Completed reduce tasks are not reset.
func (c *Coordinator) resetTask(worker string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tasks, ok := c.mapTasks.tasks[worker]
	if ok {
		for _, task := range tasks {
			if task.state == COMPLETED {
				c.mapTasks.completedTasks--
				// TODO notify reduce workers
			}
			c.files[task.file] = AVAILABLE
		}
		delete(c.mapTasks.tasks, worker)
	}

	reduceTasks, ok := c.reduceTasks.tasks[worker]
	if ok {
		for reduceTaskId, reduceTask := range reduceTasks {
			if reduceTask.state == INPROGRESS {
				c.resetReduceTaskId(reduceTaskId)
			}
		}
		delete(c.reduceTasks.tasks, worker)
	}
}

// Tries to create a map task for the worker, returns true if successful, false otherwise.
//
// A successful attempt will update the Coordinator's state with the new map task
// and fills in the reply with the map task details.
//
// An unsuccessful map task creation happens when there are no idle map tasks.
func (c *Coordinator) tryCreateMapTask(reply *ReqTaskReply, args *ReqTaskArgs) (success bool) {
	for file, available := range c.files {
		if available {
			reply.Type = MAP
			reply.MapTask = MapArgs{
				File:       file,
				Partitions: c.reduceTasks.totalTasks,
				WorkerId:   args.WorkerId,
				TaskId:     c.mapTasks.idCounter,
			}
			c.files[file] = UNAVAILABLE

			if _, ok := c.mapTasks.tasks[args.WorkerId]; !ok {
				c.mapTasks.tasks[args.WorkerId] = make(map[int]MapTask)
			}

			c.mapTasks.tasks[args.WorkerId][c.mapTasks.idCounter] = MapTask{
				state: INPROGRESS,
				file:  file,
			}
			c.mapTasks.idCounter++
			return true
		}
	}
	return false
}

// Tries to create a reduce task for the worker, returns true if successful, false otherwise.
//
// A successful attempt will update the Coordinator's state with the new reduce task
// and fills in the reply with the reduce task details.
//
// An unsuccessful attempt happens when there are no idle reduce tasks.
func (c *Coordinator) tryCreateReduceTask(reply *ReqTaskReply, args *ReqTaskArgs) (success bool) {
	reduceId := c.getAvailableReduceTaskId()
	if reduceId == -1 {
		return false
	}

	mapIds := make([]MapTaskId, c.mapTasks.totalTasks)
	i := 0
	for workerId := range c.mapTasks.tasks {
		for mapTaskId := range c.mapTasks.tasks[workerId] {
			mapIds[i] = mapTaskId
			i++
		}
	}
	reply.Type = REDUCE
	reply.ReduceTask = ReduceArgs{
		Partitions: c.reduceTasks.totalTasks,
		MapIds:     mapIds,
		ReduceId:   reduceId,
		WorkerId:   args.WorkerId,
	}

	if _, ok := c.reduceTasks.tasks[args.WorkerId]; !ok {
		c.reduceTasks.tasks[args.WorkerId] = make(map[ReduceTaskId]ReduceTask)
	}

	c.reduceTasks.tasks[args.WorkerId][reduceId] = ReduceTask{
		state: INPROGRESS,
	}
	return true
}

func (c *Coordinator) getAvailableReduceTaskId() int {
	for i, available := range c.reduceIds {
		if available {
			c.reduceIds[i] = UNAVAILABLE
			return i
		}
	}
	return -1
}

func (c *Coordinator) resetReduceTaskId(i int) {
	c.reduceIds[i] = AVAILABLE
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
	mapFiles := make(map[string]IsAvailable)
	for _, file := range files {
		mapFiles[file] = AVAILABLE
	}
	reduceIds := make(map[int]IsAvailable)
	for i := 0; i < nReduce; i++ {
		reduceIds[i] = AVAILABLE
	}
	c := Coordinator{
		mapTasks: MapTasks{
			tasks:          make(map[WorkerId]map[MapTaskId]MapTask),
			completedTasks: 0,
			totalTasks:     len(files),
		},
		reduceTasks: ReduceTasks{
			tasks:          make(map[WorkerId]map[ReduceTaskId]ReduceTask),
			completedTasks: 0,
			totalTasks:     nReduce,
		},
		files:     mapFiles,
		reduceIds: reduceIds,
	}

	c.server()
	return &c
}
