package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "6.824/mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	//reduceNum 的解释看 Coordinator.go 的 MakeCoordinator方法注释
	m := mr.MakeCoordinator(os.Args[1:], 2)
	//一秒调用一次，如果Done()返回true，说明MapReduce完全执行完毕，此时mrcoordinator.go就会退出
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
