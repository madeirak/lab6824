package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	/*
		注意coordinator中的阶段 map-reduce-alldone的转换是worker驱动的，也就是这里的循环，通过调用coordinator的rpc接口进行驱动
	*/
	keepFlag := true
	for keepFlag {
		//rpc call
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				//slave告诉coodinator此task已经done
				//rpc call
				callDone(&task)
			}
		case WaitingTask:
			{
				fmt.Println("All tasks are in progress, please wait...")
				time.Sleep(time.Second * 5)
			}
		case ReduceTask:
			{
				DoReduceTask(reducef, &task)
				//rpc call
				callDone(&task)
			}
		case ExitTask:
			{
				time.Sleep(time.Second)
				fmt.Println("master is terminated, worker is exiting...")
				keepFlag = false
			}
		}
	}

	time.Sleep(time.Second)
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

//Worker 的 rpc call 从master获取task
func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply)
	//fmt.Println("Debug2" + strconv.Itoa(len(reply.FileSlice)))

	if ok {
		//fmt.Println(reply)
	} else {
		fmt.Println("worker rpc call failed.")
	}

	return reply
}

//传入1个文件的filename，和coordinator传来的Task信息
func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	var intermeidiate []KeyValue
	//fmt.Println(len(response.FileSlice))

	//MapTask中的FileSlice数组只有一个元素
	filename := response.FileSlice[0]

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln("worker cannot open ", filename)
	}

	//// 通过io工具包获取content,作为mapf的参数
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("worker cannot read %v", filename)
	}
	file.Close()
	//mapf返回一组 []KeyValue
	intermeidiate = mapf(filename, string(content))

	//创建一个长度为rn的二维切片，外层的数组是为了对应rn个reduceTask，内层是每个reduceTask分配的具体KeyValue
	rn := response.ReducerNum
	HashedKV := make([][]KeyValue, rn)

	//kv 是 []KeyValue
	for _, kv := range intermeidiate {
		/*
		  关键：通过KeyValue的key计算hash值，使得相同的key被映射到同一个reduceTask中
		*/
		index := ihash(kv.Key) % rn
		HashedKV[index] = append(HashedKV[index], kv)
	}

	//每个mapTask生成rn个持久化文件tmp,对应rn个reduceTask
	//注意这里的mr-tmp-X-Y 中的X是map task id， Y是reduce task id
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}

		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			//将HashedKV中第一维的rn个数组以json格式持久化到mr-tmp文件中
			err := enc.Encode(kv)
			if err != nil {
				return
			}
		}
		ofile.Close()
	}
}

func DoReduceTask(reducef func(string, []string) string, response *Task) {
	reduceFileId := response.TaskId
	//对fileSlice进行排序
	intermediate := shuffle(response.FileSlice)
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("fail to create temp file", err)
	}

	//遍历reduce的中间文件file数组
	for i := 0; i < len(intermediate); {
		//reduce操作，i和j确定上下界，k来对界内的数据进行reduce
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		//reducef 传入的是map产生的key，和所有属于这个key的value数组，返回的事values数组的长度
		output := reducef(intermediate[i].Key, values)
		//写入文件
		fmt.Fprintf(tempFile, "%v, %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	//fn filename
	fn := fmt.Sprintf("mr-out-%d", reduceFileId)
	os.Rename(tempFile.Name(), fn)
}

//传入的是reduce 某个key相关的文件名数组
func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	//根据KeyValue的key来排序数组
	//ByKey()是类型转换，因为ByKey实现了sort的接口
	sort.Sort(ByKey(kva))
	return kva
}

//slave 告诉coordinator 这个task已经完成了
//返回值是coordinator的reply
func callDone(f *Task) Task {
	args := f
	reply := Task{}

	ok := call("Coordinator.MarkFinished", &args, &reply)

	if ok {
		//fmt.Println(reply)
	} else {
		fmt.Println("call failed.")
	}

	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
