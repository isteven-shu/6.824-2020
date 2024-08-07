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

var mu sync.Mutex

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type TaskPhase int

const (
	Map TaskPhase = iota
	Reduce
	Exit
	Wait
)

type Task struct {
	InputPath         string
	Phase             TaskPhase
	NReducer          int
	Id                int
	IntermediatePaths []string
	Output            string
}

type MasterTask struct {
	State         TaskState
	StartTime     time.Time
	TaskReference *Task
}

type Master struct {
	// Your definitions here.
	TaskQueue     chan *Task          // Tasks waiting to be done.
	TaskMeta      map[int]*MasterTask // All tasks' information
	MasterPhase   TaskPhase
	NReduce       int
	InputFiles    []string
	Intermediates [][]string // Map intermediate results path
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	ret = m.MasterPhase == Exit

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	m := Master{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))),
		TaskMeta:      make(map[int]*MasterTask),
		MasterPhase:   Map,
		NReduce:       nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
	}

	// Create all the map tasks, each for a single file.
	m.createMapTask()

	// Start the master.
	m.server()
	// Start a TimeOut Catcher.
	go m.catchTimeOut()
	return &m
}

func (m *Master) createMapTask() {
	// Each file is a map task
	for idx, filename := range m.InputFiles {
		task := Task{
			InputPath: filename,
			Phase:     Map,
			NReducer:  m.NReduce,
			Id:        idx,
		}
		m.TaskQueue <- &task
		m.TaskMeta[idx] = &MasterTask{
			State:         Idle,
			TaskReference: &task,
		}
	}
}

// Called after the master enters reduce phase. Create reduce tasks.
func (m *Master) createReduceTask() {
	m.TaskMeta = make(map[int]*MasterTask)
	for idx, files := range m.Intermediates {
		taskMeta := Task{
			Phase:             Reduce,
			NReducer:          m.NReduce,
			Id:                idx,
			IntermediatePaths: files,
		}
		m.TaskQueue <- &taskMeta
		m.TaskMeta[idx] = &MasterTask{
			State:         Idle,
			TaskReference: &taskMeta,
		}
	}
}

func (m *Master) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if m.MasterPhase == Exit {
			mu.Unlock()
			return
		}
		for _, masterTask := range m.TaskMeta {
			if masterTask.State == InProgress && time.Since(masterTask.StartTime) > 10*time.Second {
				m.TaskQueue <- masterTask.TaskReference
				masterTask.State = Idle
			}
		}
		mu.Unlock()
	}
}

// Called by worker
func (m *Master) AssignTask(args *ExampleArgs, reply *Task) error {
	// Check if queue is empty
	mu.Lock()
	defer mu.Unlock()
	if len(m.TaskQueue) > 0 {
		*reply = *<-m.TaskQueue
		m.TaskMeta[reply.Id].State = InProgress
		m.TaskMeta[reply.Id].StartTime = time.Now()
	} else if m.MasterPhase == Exit {
		*reply = Task{Phase: Exit}
	} else {
		*reply = Task{Phase: Wait}
	}
	return nil
}

// Called by worker
func (m *Master) TaskCompleted(task *Task, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	if task.Phase != m.MasterPhase || m.TaskMeta[task.Id].State == Completed {
		// Discard duplicate results
		return nil
	}
	// Update task state
	m.TaskMeta[task.Id].State = Completed
	go m.processTaskResult(task)
	return nil
}

func (m *Master) allTaskDone() bool {
	for _, task := range m.TaskMeta {
		if task.State != Completed {
			return false
		}
	}
	return true
}

func (m *Master) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.Phase {
	case Map:
		// Collect intermediate results path to master struct
		for reduceTaskId, filePath := range task.IntermediatePaths {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		if m.allTaskDone() {
			// Reduces can't start until the last map has finished.
			m.createReduceTask()
			m.MasterPhase = Reduce
		}
	case Reduce:
		if m.allTaskDone() {
			// Enter Exit phase after all reduce tasks are completed
			m.MasterPhase = Exit
		}
	}
}
