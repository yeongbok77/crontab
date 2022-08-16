package master

import (
	"net"
	"net/http"
	"strconv"
	"time"
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
func handleJobSave(w http.ResponseWriter, r *http.Request) {
	// 将任务保存到etcd中
	
}

// 初始化服务
func InitApiServer() (err error) {
	var (
		mux      *http.ServeMux
		listener net.Listener
	)

	// 配置路由
	mux = http.NewServeMux()

	mux.HandleFunc("/job/save", handleJobSave)

	// 启动TCP监听
	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort)); err != nil {
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
