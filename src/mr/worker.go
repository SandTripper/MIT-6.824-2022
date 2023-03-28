package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	worker_id := -1
	for {
		g_args := GetTaskArgs{
			WorkerId: worker_id,
		}
		g_reply := GetTaskReply{}

		ok := call("Coordinator.GetTask", &g_args, &g_reply)

		if !ok { //coordinator出错，退出
			return
		}

		if worker_id == -1 { //分配workerid
			worker_id = g_reply.WorkerId
		}
		task_id := g_reply.TaskId

		switch g_reply.TaskType {
		case -1: //退出命令
			return
		case 0: //等待
			time.Sleep(time.Second)
		case 1: //执行map
			nReduce := g_reply.NReduce

			content, err := os.ReadFile(g_reply.FileName) //读取map任务的输入文件内容
			if err != nil {
				log.Panic(err)
			}

			kvs := mapf(g_reply.FileName, string(content)) //处理客户传入的map方法

			tmpFiles := make([]*os.File, nReduce)  //临时文件列表
			encs := make([]*json.Encoder, nReduce) //json解析器列表

			for i := 0; i < nReduce; i++ { //创建临时文件和json解析器
				tmpFiles[i], err = os.CreateTemp(os.TempDir(), fmt.Sprintf("mr-%d-%d.*", task_id, i))
				if err != nil {
					log.Panic(err)
				}
				encs[i] = json.NewEncoder(tmpFiles[i])
			}

			for _, kv := range kvs { //按照格式将键值对分发到各个文件
				idx := ihash(kv.Key) % nReduce
				encs[idx].Encode(&kv)
			}

			for i := 0; i < nReduce; i++ { //重命名临时文件以原子性的更改文件
				err := os.Rename(tmpFiles[i].Name(), fmt.Sprintf("mr-%d-%d", task_id, i))
				if err != nil {
					log.Panic(err)
				}
			}

			d_args := DoneTaskArgs{
				WorkerId: worker_id,
				TaskId:   task_id,
			}
			d_reply := DoneTaskReply{}

			ok := call("Coordinator.DoneTask", &d_args, &d_reply) //向coordinator报告任务完成
			if !ok {
				return
			}

		case 2: //执行reduce
			kv_mp := make(map[string][]string)
			for map_task_id := 0; map_task_id < g_reply.MapTaskIdNum; map_task_id++ { //批量从map任务后的文件中却键值对
				filename := fmt.Sprintf("mr-%d-%d", map_task_id, task_id)
				file, _ := os.Open(filename)
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					if _, ok := kv_mp[kv.Key]; !ok {
						kv_mp[kv.Key] = make([]string, 0)
					}
					kv_mp[kv.Key] = append(kv_mp[kv.Key], kv.Value)

				}
			}

			tmpFile, _ := os.CreateTemp(os.TempDir(), fmt.Sprintf("mr-out-%d.*", task_id)) //创建临时文件
			for k, v := range kv_mp {                                                      //将键值对置入
				fmt.Fprintf(tmpFile, "%v %v\n", k, reducef(k, v))
			}

			err := os.Rename(tmpFile.Name(), fmt.Sprintf("mr-out-%d", task_id)) //重命名临时文件以原子性的更改文件
			if err != nil {
				log.Panic(err)
			}

			d_args := DoneTaskArgs{
				WorkerId: worker_id,
				TaskId:   task_id,
			}
			d_reply := DoneTaskReply{}

			ok := call("Coordinator.DoneTask", &d_args, &d_reply) //向coordinator报告任务完成
			if !ok {
				return
			}
		}

	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		////fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		////fmt.Printf("call failed!\n")
	}
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

	////fmt.Println(err)
	return false
}
