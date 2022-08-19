package worker

import (
	"context"
	"time"

	"github.com/yeongbok77/crontab/common"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 监听任务变化
func (jobMgr *JobMgr) watchJobs() (err error) {
	var (
		getResp    *clientv3.GetResponse
		kvpair     *mvccpb.KeyValue
		job        *common.Job
		watchChan  clientv3.WatchChan
		watchResp  clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobEvent   *common.JobEvent
	)

	// get以下/cron/jobs/下的所有任务，获取revision
	if getResp, err = jobMgr.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	for _, kvpair = range getResp.Kvs {
		if job, err = common.UnpackJob(kvpair.Value); err == nil {
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
			// TODO: 把这个任务同步给调度协程
			G_scheduler.PushJobEvent(jobEvent)
		}
	}

	// 从该revision向后监听变化事件

	// 监听协程
	go func() {
		// 从get时刻后续版本开始监听变化
		watchStartRevision := getResp.Header.Revision + 1
		// 监听/cron/jobs/目录的后续变化
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		// 处理监听事件
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: // 任务保存事件
					if job, err = common.UnpackJob(watchEvent.Kv.Value); err != nil {
						continue
					}

					// 构造一个更新 event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)

				case mvccpb.DELETE: // 任务被删除
					jobName := common.ExtractJobName(string(watchEvent.Kv.Key))

					job = &common.Job{Name: jobName}
					// 构造一个删除event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
				}
				// TODO: 推给调度协程
				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()
	return
}

func (jobMgr *JobMgr) watchKiller() {
	var (
		watchChan  clientv3.WatchChan
		watchResp  clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobEvent *common.JobEvent
		jobName string
		job *common.Job
	)
	
	// 监听协程
	go func() {
		// 监听/cron/killer/目录的后续变化
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_KILLER_DIR, clientv3.WithPrefix())
		// 处理监听事件
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: // 杀死某个任务
					jobName = common.ExtractKillerName(string(watchEvent.Kv.Key)) 	
					job = &common.Job{
						Name: jobName,
					}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL, job)
					// TODO: 推给调度协程
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: // 任务被删除
					
				}
				
			}
		}
	}()
}

// 初始化管理器
func InitJobMgr() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
	)

	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,                                  // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTime) * time.Millisecond, // 连接超时
	}

	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	// 得到 KV 和 lease 的api子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	G_jobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	// 启动监听
	G_jobMgr.watchJobs()

	// 启动监听killer
	G_jobMgr.watchKiller()
	return
}

// 创建任务执行锁
func (jobMgr *JobMgr) CreateJobLock(jobName string) (jobLock *JobLock){
	jobLock = InitJobLock(jobName, jobMgr.kv, jobLock.lease)
	return	
}