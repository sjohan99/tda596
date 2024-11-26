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

type Available bool
type MapTaskId = int
type ReduceTaskId = int
type WorkerId = string

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    MapTasks
	reduceTasks ReduceTasks
	files       map[string]Available
	reduceIds   map[ReduceTaskId]Available
}

type MapTasks struct {
	tasks          map[WorkerId]map[MapTaskId]MapTask
	completedTasks int
	totalTasks     int
	idCounter      MapTaskId
}

type ReduceTasks struct {
	tasks          map[WorkerId]map[ReduceTaskId]ReduceTask
	completedTasks int
	totalTasks     int
}

type MapTask struct {
	state     TaskState
	file      string
	mapTaskId MapTaskId
}

type ReduceTask struct {
	state  TaskState
	taskId ReduceTaskId // in an intermediate file: mr-mapId-<fileKey>
}

func (c *Coordinator) RequestTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer log.Printf("Status - Map tasks %d/%d | Reduce tasks %d/%d", c.mapTasks.completedTasks, c.mapTasks.totalTasks, c.reduceTasks.completedTasks, c.reduceTasks.totalTasks)

	if c.reduceTasks.completedTasks == c.reduceTasks.totalTasks {
		reply.Type = DONE
		return nil
	}

	if c.mapTasks.completedTasks < c.mapTasks.totalTasks {
		success := c.tryCreateMapTask(reply, args)
		if !success {
			reply.Type = WAIT
		}
		return nil
	}

	if c.reduceTasks.completedTasks < c.reduceTasks.totalTasks {
		success := c.tryCreateReduceTask(reply, args)
		if !success {
			reply.Type = WAIT
		}
		return nil
	}

	log.Panicln("This should never happen")
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

func (c *Coordinator) FinishReduce(args *ReduceFinishedArgs, reply *Empty) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	task := c.reduceTasks.tasks[args.WorkerId][args.TaskId]
	task.state = COMPLETED
	c.reduceTasks.tasks[args.WorkerId][args.TaskId] = task
	c.reduceTasks.completedTasks++
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *Empty) error {
	go c.pingWorker(args.Sockname, args.WorkerId)
	return nil
}

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
			c.files[task.file] = true
		}
		delete(c.mapTasks.tasks, worker)
	}

	reduceTasks, ok := c.reduceTasks.tasks[worker]
	if ok {
		for _, reduceTask := range reduceTasks {
			if reduceTask.state == INPROGRESS {
				c.resetReduceTaskId(reduceTask.taskId)
			}
		}
		delete(c.reduceTasks.tasks, worker)
	}
}

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
			c.files[file] = false

			if _, ok := c.mapTasks.tasks[args.WorkerId]; !ok {
				c.mapTasks.tasks[args.WorkerId] = make(map[int]MapTask)
			}

			c.mapTasks.tasks[args.WorkerId][c.mapTasks.idCounter] = MapTask{
				state:     INPROGRESS,
				file:      file,
				mapTaskId: c.mapTasks.idCounter,
			}
			c.mapTasks.idCounter++
			return true
		}
	}
	return false
}

func (c *Coordinator) tryCreateReduceTask(reply *ReqTaskReply, args *ReqTaskArgs) (success bool) {
	reduceId := c.getAvailableReduceTaskId()
	if reduceId == -1 {
		return false
	}

	mapIds := make([]MapTaskId, c.mapTasks.totalTasks)
	i := 0
	for workerId := range c.mapTasks.tasks {
		for _, task := range c.mapTasks.tasks[workerId] {
			mapIds[i] = task.mapTaskId
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
		state:  INPROGRESS,
		taskId: reduceId,
	}
	return true
}

func (c *Coordinator) getAvailableReduceTaskId() int {
	for i, available := range c.reduceIds {
		if available {
			c.reduceIds[i] = false
			return i
		}
	}
	return -1
}

func (c *Coordinator) resetReduceTaskId(i int) {
	c.reduceIds[i] = true
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
