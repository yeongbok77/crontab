package common

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/gorhill/cronexpr"
)

type Job struct {
	Name     string `json:"name"`
	Command  string `json:"command"`
	CronExpr string `json:"cronExpr"`
}

// 任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteinfo	// 执行状态
	OutPut []byte	// 脚本输出
	Err  error	// 错误
	StartTime time.Time	// 启动时间
	EndTime time.Time	// 结束时间
}

// 任务调度计划
type JobSchedulePlan struct {
	Job *Job	// 要调度的任务信息
	Expr *cronexpr.Expression	//解析好的cronexpr表达式
	NextTime time.Time	// 下次调度事件
}

// 任务执行状态
type JobExecuteinfo struct {
	Job *Job	// 任务信息
	PlanTime time.Time	// 理论上的调度时间
	RealTime time.Time	// 实际的调度时间
	CancelCtx context.Context	// 任务的上下文
	CancelFunc context.CancelFunc	// 用于取消command执行的函数
}

// 任务执行日志
type JobLog struct{
	JobName string `bson:"jobName"` // 任务名字
	Command string `bson:"command"`	
	Err string `bson:"err"`
	OutPut string `bson:"outPut"`	// 脚本输出
	PlanTime int64 `bson:"planTime"`	// 计划开始时间
	ScheduleTime int64 `bson:"scheduleTime"`	// 实际调度时间
	StartTime int64 `bson:"startTime"`	// 任务启动时间
	EndTime int64 `bson:"endTime"`
}

// HTTP接口应答
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

type JobEvent struct {
	EventType int
	Job       *Job
}

func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	// 定义一个response
	var (
		response Response
	)

	response.Errno = errno
	response.Msg = msg
	response.Data = data

	// 序列化json
	resp, err = json.Marshal(response)

	return
}

// job反序列化
func UnpackJob(value []byte) (ret *Job, err error) {
	var (
		job *Job
	)

	job = &Job{}
	if err = json.Unmarshal(value, job); err != nil {
		return
	}
	ret = job
	return
}

// 提取任务名
func ExtractJobName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

// 从 /cron/killer/jobname 里提取jobName
func ExtractKillerName(killerKey string) string {
	return strings.TrimPrefix(killerKey, JOB_KILLER_DIR)
}

// 任务变化事件有2种：PUT DELETE
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{
		EventType: eventType,
		Job:       job,
	}
}

func BuildJobSchedulePlan(job *Job) (jobSchedulePlan *JobSchedulePlan, err error) {
	var (
		expr *cronexpr.Expression
	)

	// 解析job的cron表达式
	if expr,err = cronexpr.Parse(job.CronExpr); err!=nil {
		return
	}

	// 生成任务调度计划
	jobSchedulePlan = &JobSchedulePlan{
		Job: job,
		Expr: expr,
		NextTime: expr.Next(time.Now()),
	}

	return 
}

// 构造执行状态信息
func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulePlan) (jobExecuteinfo *JobExecuteinfo) {
	jobExecuteinfo = &JobExecuteinfo{
		Job: jobSchedulePlan.Job,
		PlanTime: jobSchedulePlan.NextTime,	// 计划调度时间
		RealTime: time.Now(),	// 真实调度时间
	}
	ctx, cancelFunc := context.WithCancel(context.TODO())
	jobExecuteinfo.CancelCtx = ctx
	jobExecuteinfo.CancelFunc = cancelFunc
	return
}