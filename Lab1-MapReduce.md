# Lab1 - MapReduce

## Structs

```go
  type TaskState int  // The master needs to keep track of the state of each task.

const (
	Idle TaskState = iota  // Initial state of each task.
	InProgress
	Completed
)

type TaskPhase int  // The task phase is to instruct workers.

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
	TaskMeta      map[int]*MasterTask // All tasks' meta
	MasterPhase   TaskPhase
	NReduce       int
	InputFiles    []string
	Intermediates [][]string // Map intermediate results path
}
```

## Master

### Register RPC

Package `net/rpc` provides the ability of accessing an object's methods through network.

Register Master to rpc so the methods of master can be called remotely.

```go
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
```

Two methods will be called by workers:

```go
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
```

### Master Start Up

```go
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
```

1. Create the Master;
2. Create a Map task for each input file and also create the corresponding metadata;

```go
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
```

3. Start a master thread that listens RPCs from workers (code as shwon in Register RPC);
4. Start a thread to check time out;

## Worker

### RPC

```go
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
```

The methods will make rpc calls to the master:

```
// Worker asks for a task
func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	call("Master.AssignTask", &args, &reply)
	return reply
}

func TaskCompleted(task *Task) {
        // Send an RPC request to tell the master the task is complete
	reply := ExampleReply{}
	call("Master.TaskCompleted", task, &reply)
}
```

### Worker

Each worker thread is basically a loop:

```go
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// Ask for a task from Master.
		task := getTask()

		// Call Mapper or Reducer according to the task state
		// The extra 2 states are for wait and exit
		switch task.Phase {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}
}
```

Based on the assigned task's phase

```go
func mapper(task *Task, mapf func(string, string) []KeyValue) {
	// Read file content
	file, err := os.Open(task.InputPath)
	if err != nil {
		log.Fatalf("cannot open %v", task.InputPath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.InputPath)
	}
	file.Close()

	// Call mapf
	intermediates := mapf(task.InputPath, string(content))

	// Partiton the result and store them to local disk
	buckets := make([][]KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReducer
		buckets[slot] = append(buckets[slot], intermediate)
	}
	mapOutput := make([]string, 0)
	for i := 0; i < task.NReducer; i++ {
		mapOutput = append(mapOutput, writeToLocalFile(task.Id, i, &buckets[i]))
	}
	// Record the path of all intermediates
	task.IntermediatePaths = mapOutput
	TaskCompleted(task)
}

func reducer(task *Task, reducef func(string, []string) string) {
	// Read file content
	kva := []KeyValue{}
	for _, fileName := range task.IntermediatePaths {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", task.InputPath)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	// Sort intermediates by key.
	sort.Sort(ByKey(kva))

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	// call Reduce on each distinct key in intermediate[],
	// and print the result
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.Id)
	os.Rename(tempFile.Name(), oname)
	task.Output = oname
	TaskCompleted(task)
}

// X is the Map task number, and Y is the reduce task number.
func writeToLocalFile(x int, y int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}
```
