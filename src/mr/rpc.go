package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task struct {
	TaskType   TaskType //任务类型，map还是reduce
	TaskId     int
	ReducerNum int      //reducer的数量，用于计算任务映射的hash
	FileSlice  []string //task关联的输入文件, MapTask 是一个task对应一个源文件， ReduceTask是一个task对应对个中间文件
}

//TaskArgs rpc应该传入的参数，实际上因为worker只获取一个任务，应该什么都不传
type TaskArgs struct{}

//TaskType 对于下方枚举任务的父类型
type TaskType int

//Phase对于分配任务阶段的父类型
type Phase int

//State任务状态的父类型
type State int

//枚举任务的类型
const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask //表示此时任务都分发完了，但是任务还没完成，阶段未改变
	ExitTask    //控制worker退出的假任务
	/*
		1.首先master在处理结束后会通过Done()方法自动退出，而slave此时无法判断何时该退出，
		一种做法是，通过worker的rpc请求的返回值，当worker请求不到master时，假定master因为任务全部执行而退出，
		所以worker自己也可以退出。
		2.第二种方法就是master主动在退出前给slave发出ExitTask
	*/
)

//枚举阶段的类型
const (
	MapPhase    Phase = iota //此阶段在分发MapTask
	ReducePhase              //此阶段在分发ReduceTask
	AllDone                  //此阶段已完成
)

//任务task的状态类型
const (
	Working State = iota //当前阶段在工作
	Waiting              //当前阶段在等待执行
	Done                 //当前阶段已经做完
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
