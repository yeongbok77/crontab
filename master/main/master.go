package main

import (
	"flag"
	"fmt"
	"runtime"

	"github.com/yeongbok77/crontab/master"
)

var (
	confFile string
)

// 解析命令行参数
func initArgs() {
	// master -config ./master.json
	flag.StringVar(&confFile, "config", "./master.json", "指定master.json")
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
	if err = master.InitConfig(confFile); err != nil {
		return
	}

	// 启动HTTP服务
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	return

ERR:
	fmt.Println(err)

}
