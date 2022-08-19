package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/yeongbok77/crontab/worker"
)

var (
	confFile string
)

// 解析命令行参数
func initArgs() {
	// worker -config ./master.json
	flag.StringVar(&confFile, "config", "./worker.json", "worker.json")
	flag.Parse()
}

// 配置线程数量
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var (
		err error
	)
	// 初始化命令行参数
	initArgs()

	// 初始化线程
	initEnv()

	// 初始化配置
	if err = worker.InitConfig(confFile); err != nil {
		return
	}

	// 初始化调度器
	if err = worker.InitScheduler(); err!=nil {
		return
	}

	// 初始化任务管理器（etcd）
	if err = worker.InitJobMgr(); err!=nil {
		goto ERR
	}

	log.Println("启动成功")

	for {
		time.Sleep(1*time.Second)
	}

	return

ERR:
	fmt.Println("启动失败",err)

}
