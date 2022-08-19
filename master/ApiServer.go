package master

import (
	"encoding/json"
	"log"

	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/yeongbok77/crontab/common"
)

// HTTP接口
type ApiServer struct {
	httpServer *http.Server
}

var (
	// 单例对象
	G_apiServer *ApiServer
)


// 保存路由接口
// POST job={"name":"job1", "command":"echo hello", "cronExpr":"*****"}
func handleJobSave(w http.ResponseWriter, r *http.Request) {
	// 将任务保存到etcd中
	var (
		postJob string
		job     common.Job
		oldJob  *common.Job
		bytes   []byte
		err     error
	)
	// 解析表单
	if err = r.ParseForm(); err != nil {
		log.Println("/job/save  FAILED   PareForm failed")
		goto ERR
	}

	// 取表单中的job字段
	postJob = r.PostFormValue("job")

	// 反序列化job

	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		log.Println("/job/save  FAILED   json.Unmarshal failed")
		goto ERR
	}

	// 保存到etcd
	oldJob, err = G_jobMgr.SaveJob(&job)
	if err != nil {
		log.Println("/job/save  FAILED   etcd work failed")
		goto ERR
	}

	// 返回正常应答
	bytes, err = common.BuildResponse(0, "success", &oldJob)
	if err == nil {
		w.Write(bytes)
	}
	log.Println("/job/save  SUCCESS   ", http.StatusOK)
	return

ERR:
	// 异常应答
	bytes, err = common.BuildResponse(-1, err.Error(), nil)
	if err == nil {
		log.Println("/job/save  FAILED   ", http.StatusOK)
		w.Write(bytes)
	}
	return
}

// 删除任务接口
// POST表单传递key  name=job1
func handleJobDelete(w http.ResponseWriter, r *http.Request) {
	var (
		bytes   []byte
		oldJob  *common.Job
		jobName string // 要删除的任务名
		err     error
	)
	// 解析表单
	if err = r.ParseForm(); err != nil {
		log.Println("/job/delete    FAILED    r.ParseForm")
		goto ERR
	}

	// 获取表单数据
	jobName = r.PostFormValue("name")

	// 删除任务
	if oldJob, err = G_jobMgr.DeleteJob(jobName); err != nil {
		log.Println("/job/delete    FAILED    G_jobMgr.DeleteJob")
		goto ERR
	}

	// 正常应答
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		log.Println("/job/delete    SUCCESS    ", http.StatusOK)
		w.Write(bytes)
	}

	return

ERR:
	// 异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
	log.Println("/job/delete    FAILED")
	return
}

// 列出所有crontab任务
func handleJobList(w http.ResponseWriter, r *http.Request) {
	var (
		jobList []*common.Job
		bytes   []byte
		err     error
	)

	// etcd操作，列出所有任务
	if jobList, err = G_jobMgr.ListJobs(); err != nil {
		log.Println("/job/list    FAILED    G_jobMgr.ListJobs")
		goto ERR
	}

	// 正常应答
	if bytes, err = common.BuildResponse(0, "success", jobList); err == nil {
		log.Println("/job/list    SUCCESS    ", http.StatusOK)
		w.Write(bytes)
	}
	return

ERR:
	// 异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
	log.Println("/job/list    FAILED")

	return
}

// 强制杀死某个任务
func handleJobKill(w http.ResponseWriter, r *http.Request) {
	var (
		bytes []byte
		jobName string
		err error
	)

	// 解析表单
	if err = r.ParseForm(); err != nil {
		log.Println("/job/kill    FAILED    r.ParseForm")
		goto ERR
	}

	// 获取表单数据
	jobName = r.PostFormValue("name")

	// etcd操作，通知worker
	if err = G_jobMgr.Kill(jobName); err!=nil {
		log.Println("/job/kill    FAILED    G_jobMgr.Kill")
		goto ERR
	}

	// 正常应答
	if bytes,err = common.BuildResponse(0,"success", nil);err==nil {
		log.Println("/job/list    SUCCESS    ", http.StatusOK)
		w.Write(bytes)
	}
	return

ERR:	
	// 异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
	log.Println("/job/kill    FAILED")

	return

}


// 初始化服务
func InitApiServer() (err error) {
	var (
		mux      *http.ServeMux
		listener net.Listener
		staticDir http.Dir
		staticHandler http.Handler
	)

	// 配置路由
	mux = http.NewServeMux()

	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)

	// 静态文件目录
	staticDir= http.Dir("./webroot")
	staticHandler = http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/", staticHandler))

	// 启动TCP监听
	if listener, err = net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(G_config.ApiPort)); err != nil {
		return
	}

	// 创建一个http服务
	httpServer := &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}

	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	// 启动服务端
	go httpServer.Serve(listener)

	return

}
