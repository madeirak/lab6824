package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int            //reducer 个数
	TaskId            int            //用于生成task唯一id，保存着当前最新的taskId
	CoordinatorPhase  Phase          //整体系统处于的任务阶段(coordinator)
	TaskChannelReduce chan *Task     //reduce任务的channel
	TaskChannelMap    chan *Task     //map任务的channel
	taskMetaHolder    TaskMetaHolder //存着全部task信息
	files             []string       //传入的源文件数组
}

//自增生成task id
func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	//悲观锁 因为涉及在将task放入channel后对task状态、coordinator状态的并发修改,
	mu.Lock()
	defer mu.Unlock()

	switch c.CoordinatorPhase {
	case MapPhase:
		{
			if len(c.TaskChannelMap) > 0 {
				//网上的
				*reply = *<-c.TaskChannelMap
				//我写的（是错的）
				//reply = <-c.TaskChannelMap
				//fmt.Println("Debug1" + strconv.Itoa(len(reply.FileSlice)))

				//改变task的状态(state)为Working  Waiting -> Working
				c.taskMetaHolder.runTask(reply.TaskId)
			} else {
				//通过一个假任务WaitingTask告诉Slave此时无task可用
				reply.TaskType = WaitingTask
				//检查当前状态下的task是否全部完成了
				//此处就用到了taskMetaHolder里存储的所有的task的状态信息
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
					if c.CoordinatorPhase == ReducePhase {
						//进行进入reduce phase前的准备工作
						//创建reduce task并放入channel
						c.makeReduceTasks()
					}
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.TaskChannelReduce) > 0 {
				//我写的 (是错的)
				//reply = <-c.TaskChannelReduce
				//网上的
				*reply = *<-c.TaskChannelReduce
				//Waiting -> Working
				c.taskMetaHolder.runTask(reply.TaskId)
			} else {
				//通过一个假任务WaitingTask告诉Slave此时无task可用
				reply.TaskType = WaitingTask
				//检查当前状态下的task是否全部完成了
				//此处就用到了taskMetaHolder里存储的所有的task的状态信息
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{
			//通过一个假任务 ExitTask告诉slave
			reply.TaskType = ExitTask
		}
	default:
		panic("the coordinator phase passed in is undefined")
	}

	return nil

}

//进入下一个phase,coordinator的状态转换
func (c *Coordinator) toNextPhase() {
	if c.CoordinatorPhase == MapPhase {
		c.CoordinatorPhase = ReducePhase
	} else if c.CoordinatorPhase == ReducePhase {
		c.CoordinatorPhase = AllDone
	}
}

//持有全部任务的元数据,map,key是taskId，value是taskMetaInfo
type TaskMetaHolder struct {
	//key是taskId，value是task metaInfo
	MetaMap map[int]*TaskMetaInfo
}

//任务的元数据,将动态信息"task状态"与静态信息"TaskType、TaskId、ReducerNum、Filename"分开封装
type TaskMetaInfo struct {
	state     State     //任务的状态
	TaskAddr  *Task     //指针类型，为了在task从channel取出来并完成之后，还能通过地址标记该task已完成
	StartTime time.Time //记录任务开始时间，用于 Worker crash的处理
}

//将TaskMetaInfo放到Coordinator的TaskMetaHolder中
//返回值表示当前task是否被成功添加到coodinator的holder中
func (t *TaskMetaHolder) storeMeta(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskAddr.TaskId
	meta := t.MetaMap[taskId]
	if meta != nil {
		fmt.Println("meta already contains task id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}

//检查当前状态（map/redcue）下coordinator中的所有task的执行情况，来判断是否要进度下一个阶段
func (t *TaskMetaHolder) checkTaskDone() bool {
	//统计当前coordinator中的所有task的执行情况
	var (
		mapUnDoneNum    = 0
		reduceUnDoneNum = 0
	)

	for _, task := range t.MetaMap {
		if task.TaskAddr.TaskType == MapTask && task.state != Done {
			mapUnDoneNum++
		} else if task.TaskAddr.TaskType == ReduceTask && task.state != Done {
			reduceUnDoneNum++
		}
	}

	if reduceUnDoneNum == 0 || mapUnDoneNum == 0 {
		return true
	} else {
		return false
	}
}

//改变task的状态为Working
func (t *TaskMetaHolder) runTask(taskId int) {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		panic("coordinator change task state to Working error occurred.")
	}
	//在task被从channel中取出之后，设置task的状态和开始时间
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()

	fmt.Printf("task %d is running.\n", taskInfo.TaskAddr.TaskId)
}

//提供给外部的rpc handler，标记一个task已经执行完成  Working -> Done
func (c *Coordinator) MarkFinished(args *Task, task *Task) error {
	//悲观锁 因为涉及在将task放入channel后对task状态的并发修改
	mu.Lock()
	defer mu.Unlock()

	switch args.TaskType {
	case MapTask:
		{
			meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]

			if ok && meta.state == Working {
				meta.state = Done
				fmt.Printf("%v %d is finished.", args.TaskType, meta.TaskAddr.TaskId)
			}
		}
	case ReduceTask:
		{
			meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]

			if ok && meta.state == Working {
				meta.state = Done
				fmt.Printf("%v %d is finished.", args.TaskType, meta.TaskAddr.TaskId)
			}
		}
	default:
		{
			panic("the task type passed in is undefined.")
		}
	}
	return nil
}

//
//an example RPC handler.
//
//the RPC argument and reply types are defined in rpc.go.
//
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
//一秒被mrcoordinator.go调用一次，如果Done()返回true，说明MapReduce完全执行完毕，此时mrcoordinator.go就会退出
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.CoordinatorPhase == AllDone {
		fmt.Println("All tasks are finished,the coordinator will be exit! !")
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
//files 是传入的所有源文件的数组，元素是文件名s
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.ReducerNum = nReduce
	//这里或许用1个channel也可以
	c.TaskChannelMap = make(chan *Task, len(files))
	c.TaskChannelReduce = make(chan *Task, nReduce)
	c.CoordinatorPhase = MapPhase
	c.files = files
	/**
	  这里的taskholder的初始长度是nReduce+len(files)，其实就是所有的MapTask、ReduceTask之和
	  进一步，其实在计算reduce的hash值时用到的nReduce不是实际的Reducer的个数，而是逻辑上想把reduce阶段分成几个reduce任务来调度执行
	  比如极端情况下，一个物理上的reduce机器可以顺序执行所有的reduceTask
	*/
	c.taskMetaHolder = TaskMetaHolder{MetaMap: make(map[int]*TaskMetaInfo, nReduce+len(files))}

	//coordinator创建map task，并放入channel
	c.makeMapTasks(files)

	c.server()

	go c.CrashDetector()

	return &c
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 3)

		//需要并发改task state 加锁
		mu.Lock()
		if c.CoordinatorPhase == AllDone {
			mu.Unlock()
			break
		}

		for _, task := range c.taskMetaHolder.MetaMap {
			if task.state == Working && time.Since(task.StartTime).Seconds() > 10 {
				fmt.Printf("the task %d is crashed, it will be coordinating.", task.TaskAddr.TaskId)
				switch task.TaskAddr.TaskType {
				case MapTask:
					{
						c.TaskChannelMap <- task.TaskAddr
						task.state = Waiting
					}
				case ReduceTask:
					{
						c.TaskChannelReduce <- task.TaskAddr
						task.state = Waiting
					}
				}
			}
		}

		mu.Unlock()
	}
}

//创建一个map task并放入map channel
//传入的是file数组，一个file创建一个map task
func (c *Coordinator) makeMapTasks(files []string) {
	for _, file := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			FileSlice:  []string{file},
		}

		//保存task的初始状态
		taskMetaInfo := TaskMetaInfo{
			state:    Waiting, // 初始状态是"待执行"
			TaskAddr: &task,   //任务地址
		}

		//将一个file以Task的形式放入TaskMetaHolder Map
		c.taskMetaHolder.storeMeta(&taskMetaInfo)

		fmt.Println("make a map task :", &task)

		//放入channel
		c.TaskChannelMap <- &task
	}
}

//循环创建reduce task 并加入channel
func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskId:   id,
			TaskType: ReduceTask,
			//根据reducerId找到reducer关联的fileName数组
			FileSlice: selectReduceName(i),
		}

		taskInfo := TaskMetaInfo{
			state:    Waiting,
			TaskAddr: &task,
		}

		c.taskMetaHolder.storeMeta(&taskInfo)

		c.TaskChannelReduce <- &task
	}
}

//在工作目录中的所有中间文件中找到属于当前reducer的中间文件汇总后返回
func selectReduceName(reducerId int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reducerId)) {
			s = append(s, fi.Name())
		}
	}

	return s
}
