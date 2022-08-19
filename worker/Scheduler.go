package worker

import (
	"time"

	"github.com/yeongbok77/crontab/common"
)

// 任务调度器
type Scheduler struct {
	jobEventChan chan *common.JobEvent	// etcd的任务事件队列
	jobPlanTable map[string]*common.JobSchedulePlan	// 任务调度计划表
	jobExecutingTable map[string]*common.JobExecuteinfo	// 任务执行表
	jobResultChan chan *common.JobExecuteResult // 任务结果队列
}

var (
	G_scheduler *Scheduler
)

// 处理任务事件
func (scheduler *Scheduler)handleJobEvent(jobEvent *common.JobEvent) {
	var (
		jobSchedulePlan *common.JobSchedulePlan	
		jobExisted	bool
		err error
		jobExecuteInfo *common.JobExecuteinfo
		jobExecuting bool
	)
	switch jobEvent.EventType {
		case common.JOB_EVENT_SAVE:	// 保存任务事件
			if jobSchedulePlan,err = common.BuildJobSchedulePlan(jobEvent.Job); err!=nil {
				return
			}
			scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
		case common.JOB_EVENT_DELETE:	// 删除任务事件
			if jobSchedulePlan, jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted {
				delete(scheduler.jobPlanTable, jobEvent.Job.Name)
			}
		case common.JOB_EVENT_KILL:	// 强杀任务事件
			if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobEvent.Job.Name]; jobExecuting {
				jobExecuteInfo.CancelFunc()
			}
	}
}

// 重新计算任务调度状态
func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration){
	var (
		jobPlan *common.JobSchedulePlan
		now time.Time
		nearTime *time.Time	// 最近一个要过期的任务的执行时间
	)

	if len(scheduler.jobPlanTable)==0 {
		scheduleAfter = 1*time.Second
		return
	}

	// 当前时间
	now  = time.Now()

	// 1.遍历所有任务
	for _,jobPlan = range scheduler.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			// TODO: 尝试执行任务
			scheduler.TryStartJob(jobPlan)
			// 更新下一次执行时间
			jobPlan.NextTime = jobPlan.Expr.Next(now)	
		}

		// 统计最近一个要过期的任务的执行时间
		if nearTime==nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}
	

	scheduleAfter = (*nearTime).Sub(now)
	return
}

// 尝试执行任务
func (scheduler *Scheduler)TryStartJob(jobPlan *common.JobSchedulePlan) {
	var (
		jobExecuteInfo *common.JobExecuteinfo
		jobExecuting bool
	)
	// 如果任务正在执行就跳过本次调度
	if jobExecuteInfo,jobExecuting= scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		return
	}

	// 构件执行信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	// 保存执行状态
	scheduler.jobExecutingTable[jobExecuteInfo.Job.Name] = jobExecuteInfo

	// 执行任务
	// TODO: 
	G_executor.ExecuteJob(jobExecuteInfo)
}

// 处理任务结果
func (scheduler *Scheduler)handleJobResult(jobResult *common.JobExecuteResult) {
	var (
		jobLog *common.JobLog
	)
	// 删除执行状态
	delete(scheduler.jobExecutingTable, jobResult.ExecuteInfo.Job.Name)

	// 生成执行日志
	if jobResult.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &common.JobLog{
			JobName: jobResult.ExecuteInfo.Job.Name,
			Command: jobResult.ExecuteInfo.Job.Command,
			OutPut: string(jobResult.OutPut),
			PlanTime: jobResult.ExecuteInfo.PlanTime.UnixNano()/1000/1000,
			ScheduleTime: jobResult.ExecuteInfo.RealTime.UnixNano()/1000/1000,
			StartTime: jobResult.StartTime.UnixNano()/1000/1000,
			EndTime: jobResult.EndTime.UnixNano()/1000/1000,
		}
	}
	if jobResult.Err != nil {
		jobLog.Err = jobResult.Err.Error()
	} else {
		jobLog.Err = ""
	}
	
}

// 调度协程
func (scheduler *Scheduler)scheduleLoop() {
	var (
		jobEvent *common.JobEvent
	)

	scheduleAfter := scheduler.TrySchedule()

	scheduleTimer := time.NewTimer(scheduleAfter)

	for {
		select {
		case jobEvent = <- scheduler.jobEventChan:	// 监听任务变化事件
			// 对内存种维护的任务列表做增删改查
			scheduler.handleJobEvent(jobEvent)
		case <- scheduleTimer.C:	//看是否到期
		case jobResule := <- scheduler.jobResultChan:	// 任务结果
			scheduler.handleJobResult(jobResule)
		}
		// 调度一次任务
		scheduleAfter = scheduler.TrySchedule()
		// 重置定时器
		scheduleTimer.Reset(scheduleAfter)
	}
}

// 推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

// 初始化任务调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan: make(chan *common.JobEvent, 1000),
		jobPlanTable: make(map[string]*common.JobSchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteinfo),
		jobResultChan: make(chan *common.JobExecuteResult, 1000),
	}

	// 启动调度协程
	go G_scheduler.scheduleLoop()

	return
}


// 接收任务执行结果
func (scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	scheduler.jobResultChan <- jobResult
}