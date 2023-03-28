package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	status          int              //进行的状态
	input_files     []string         //输入文件
	undo_tasks      *list.List       //未分配的任务
	doing_tasks     map[int]struct{} //正在执行的任务
	map_task_id_num int              //map任务的数量
	worker_status   map[int]int      //worker的状态
	worker_cnt      int              //worker的数量，同时用于分配id
	nReduce         int              //reduce任务的数量
	lock            sync.Mutex       //互斥锁
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	//加锁避免竞争
	c.lock.Lock()
	defer c.lock.Unlock()

	if args.WorkerId == -1 { //如果是未分配id的worker，分配id
		c.worker_cnt++
		args.WorkerId = c.worker_cnt
	}

	reply.WorkerId = args.WorkerId
	reply.NReduce = c.nReduce

	if c.status == 2 { //已结束所有工作,通知worker关闭
		reply.TaskType = -1
		return nil
	}

	if c.undo_tasks.Len() != 0 { //还有工作可做
		front := c.undo_tasks.Front()
		task_id := front.Value.(int)                    //获取任务id
		reply.TaskId = c.undo_tasks.Remove(front).(int) //从链表中删除
		c.doing_tasks[task_id] = struct{}{}             //添加至正在工作

		if c.status == 0 {
			reply.TaskType = 1
			reply.FileName = c.input_files[reply.TaskId]

			go func(c *Coordinator, task_id int, worker_id int) { //检查掉线函数
				time.Sleep(time.Duration(time.Second * 10))
				c.lock.Lock()
				defer c.lock.Unlock()
				if c.status != 0 {
					return
				}
				if _, ok := c.doing_tasks[task_id]; ok {
					delete(c.doing_tasks, task_id)
					c.undo_tasks.PushBack(task_id)
					c.worker_status[worker_id] = -1
				}
			}(c, task_id, args.WorkerId)

		} else if c.status == 1 {
			reply.TaskType = 2
			reply.MapTaskIdNum = c.map_task_id_num

			go func(c *Coordinator, task_id int, worker_id int) { //检查掉线函数
				time.Sleep(time.Duration(time.Second * 10))
				c.lock.Lock()
				defer c.lock.Unlock()
				if c.status != 1 {
					return
				}
				if _, ok := c.doing_tasks[task_id]; ok {
					delete(c.doing_tasks, task_id)
					c.undo_tasks.PushBack(task_id)
					c.worker_status[worker_id] = -1
				}
			}(c, task_id, args.WorkerId)

		}
	} else {
		reply.TaskType = 0 //没有未分配的工作，让worker等待
	}

	return nil
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.worker_status[args.WorkerId] == -1 { //如果该工作超时，则忽略
		c.worker_status[args.WorkerId] = 1
		return nil
	}
	if c.status != 0 && c.status != 1 { //状态不合法
		return nil
	}

	task_id := args.TaskId

	if _, ok := c.doing_tasks[task_id]; ok {
		delete(c.doing_tasks, task_id)                          //从正在工作删除
		if c.undo_tasks.Len() == 0 && len(c.doing_tasks) == 0 { //所有工作已完成
			if c.status == 0 {
				c.status = 1
				for i := 0; i < c.nReduce; i++ {
					c.undo_tasks.PushBack(i)
				}
			} else if c.status == 1 {
				c.status = 2
			}
		}
	}

	return nil
}

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
	ret := false

	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.status == 2 {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.status = 0

	c.input_files = make([]string, len(files))
	c.undo_tasks = list.New()
	c.doing_tasks = make(map[int]struct{})

	c.map_task_id_num = len(files)

	c.worker_status = make(map[int]int)
	c.worker_cnt = 0
	c.nReduce = nReduce
	copy(c.input_files, files)
	for i := 0; i < len(files); i++ {
		c.undo_tasks.PushBack(i)
	}

	c.server()
	return &c
}
