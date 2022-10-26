package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TaskMap    = 0
	TaskReduce = 1
	TaskWait   = 2
	TaskEnd    = 3
)

type Task struct {
	FileName  string
	Id        int
	StartTime time.Time
	Status    TaskStatus
	Wait      chan int
	JobType   int
}
type TaskStatus struct {
	Timeout bool
	Done    bool
}
type Coordinator struct {
	// Your definitions here.
	Files          []string
	NReduce        int
	NMap           int
	Tasks          chan Task
	HeartbeatCh    chan heartbeatMsg
	ReportCh       chan reportMsg
	DoneCh         chan int
	DoneTask       []Task
	UnDoneTask     []Task
	MapDone        bool
	ReduceDone     bool
	DoneTaskCount  int
	ReduceDoneLock sync.Mutex
}

type heartbeatMsg struct {
	Response *HeartbeatResponse
	Ok       chan struct{}
}

type reportMsg struct {
	Request *ReportRequest
	Ok      chan struct{}
}
type ReportRequest struct {
	Task Task
}

type HeartbeatResponse struct {
	Task    Task
	NReduce int
	NMap    int
}
type HeartbeatRequest struct {
}
type ReportResponse struct {
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	c.ReduceDoneLock.Lock()
	ret := c.ReduceDone
	c.ReduceDoneLock.Unlock()

	// Your code here.
	if ret {
		log.Println("asked whether i am done, replying yes...")
	} else {
		log.Println("asked whether i am done, replying no...")
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Files = files
	c.NReduce = nReduce
	c.NMap = len(files)
	tasks := make(chan Task, len(files)+nReduce)
	c.UnDoneTask = make([]Task, 0, c.NMap+c.NReduce)
	// add map task
	for i := 0; i < len(files); i++ {
		task := Task{
			FileName:  files[i],
			Id:        i,
			StartTime: time.Now(),
			JobType:   0,
			Status: TaskStatus{
				Done:    false,
				Timeout: false,
			},
			Wait: make(chan int),
		}
		tasks <- task
		c.UnDoneTask = append(c.UnDoneTask, task)
	}
	c.DoneTask = make([]Task, 0, c.NMap+c.NReduce)
	c.Tasks = tasks
	c.DoneCh = make(chan int, 10)
	c.ReportCh = make(chan reportMsg)
	c.HeartbeatCh = make(chan heartbeatMsg)
	c.MapDone = false
	c.ReduceDone = false
	c.DoneTaskCount = 0
	c.server()
	//handle heartbeat and report
	go c.schedule()
	return &c
}

func (c *Coordinator) AskTask(request *HeartbeatRequest, reply *HeartbeatResponse) error {
	reply.Task = <-c.Tasks
	//No task to give
	if reply.Task.StartTime.IsZero() {
		reply.Task = Task{
			JobType: 2,
		}
	}
	reply.NMap = c.NMap
	reply.NReduce = c.NReduce
	msg := heartbeatMsg{reply, make(chan struct{})}
	reply.NReduce = c.NReduce
	// give task to worker
	c.HeartbeatCh <- msg
	// wait worker finish
	<-msg.Ok
	go c.WaitTaskDone(c.UnDoneTask[reply.Task.Id].Wait, reply.Task)
	reply.Task.StartTime = time.Now()
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	reportTime := time.Now()
	if reportTime.Sub(request.Task.StartTime) > 20*time.Second {
		c.Tasks <- request.Task
		return errors.New("task timout")
	}
	taskId := request.Task.Id
	c.UnDoneTask[taskId].Wait <- 1
	msg := reportMsg{request, make(chan struct{})}
	c.DoneCh <- 1
	c.ReportCh <- msg
	<-msg.Ok
	return nil
}

func (c *Coordinator) schedule() {
	for {
		select {
		case msg := <-c.HeartbeatCh:
			msg.Ok <- struct{}{}
		case msg := <-c.ReportCh:
			// map task finish,add reduce task
			if c.DoneTaskCount == c.NMap-1 && !c.MapDone {
				c.AddReduceTask()
				c.MapDone = true
			}
			Done := <-c.DoneCh
			if Done == 1 {
				c.DoneTaskCount++
				if c.DoneTaskCount == c.NReduce+c.NMap {
					c.ReduceDoneLock.Lock()
					c.ReduceDone = true
					c.ReduceDoneLock.Unlock()
				}
			}
			msg.Ok <- struct{}{}
		}
	}
}

func (c *Coordinator) AddReduceTask() {
	// add reduce task
	for i := 0; i < c.NReduce; i++ {
		task := Task{
			//FileName:  c.Files[i],
			Id:        c.NMap + i,
			StartTime: time.Now(),
			JobType:   1,
			Status: TaskStatus{
				Done:    false,
				Timeout: false,
			},
			Wait: make(chan int),
		}
		c.Tasks <- task
		c.UnDoneTask = append(c.UnDoneTask, task)
	}
}

func (c *Coordinator) WaitTaskDone(ch chan int, task Task) {
	select {
	case <-time.After(time.Second * 20):
		fmt.Println("Task 超时")
		c.Tasks <- task
	case <-ch:
		fmt.Println("Task finish")
	}
}
