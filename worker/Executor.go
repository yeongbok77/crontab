package worker

import (
	"math/rand"
	"os/exec"
	"time"

	"github.com/yeongbok77/crontab/common"
)

type Executor struct{

}

var (
	G_executor *Executor
)

// 执行一个任务
func (executor *Executor) ExecuteJob(info *common.JobExecuteinfo) {
	go func ()  {
		var (
			cmd *exec.Cmd
			outPut []byte
			result *common.JobExecuteResult
			err error
			jobLock *JobLock
		)

		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			OutPut: make([]byte, 0),
		}

		// 获取分布式锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

		// 随机睡眠（0-1）
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond) 


		err = jobLock.TryLock()
		defer jobLock.Unlock()
		

		if err !=nil {
			result.Err = err
			result.EndTime = time.Now()
		} else {
			// 任务开始时间
			result.StartTime = time.Now()
			// 执行shell命令
			cmd = exec.CommandContext(info.CancelCtx, "/bin/bash","-c",info.Job.Command)
			
			// 执行并捕获输出
			outPut,err = cmd.CombinedOutput()
			
			// 任务结束时间
			result.EndTime = time.Now()
			result.OutPut = outPut
			result.Err = err
			
			// 将执行结果传递会调度器
			G_scheduler.PushJobResult(result)

			
		}
	}()
}

func InitExecutor() (err error) {
	G_executor = &Executor{}
	return
}
