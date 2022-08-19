package master

import (
	"context"
	"encoding/json"

	"time"

	"github.com/yeongbok77/crontab/common"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 初始化管理器
func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
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

	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}

// 保存任务
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	var (
		jobKey    string
		jobValue  []byte
		oldJobObj common.Job
	)

	//etcdd的保存key
	jobKey = common.JOB_SAVE_DIR + job.Name

	// 学到了： job里有多个字段，这时可以序列化成json，存储json
	// 任务信息json
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}

	// 保存到etcd
	putResp, err := jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV())
	if err != nil {
		return
	}

	// 如果是更新，那么返回旧值
	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}

	return

}

// 删除任务
func (jobMgr *JobMgr) DeleteJob(jobName string) (oldJob *common.Job, err error) {

	var (
		oldJobObj common.Job
	)
	// 要删除的key
	jobKey := common.JOB_SAVE_DIR + jobName

	// 删除操作
	deleteResp, err := jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV())
	if err != nil {
		return
	}

	// 返回旧值
	if len(deleteResp.PrevKvs) != 0 {
		if err = json.Unmarshal(deleteResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}

	return

}

// 列举任务
func (jobMgr *JobMgr) ListJobs() (jobList []*common.Job, err error) {
	var (
		dirKey  string
		kvPair  *mvccpb.KeyValue
		getResp *clientv3.GetResponse
		job     *common.Job
	)

	// 任务保存的目录
	dirKey = common.JOB_SAVE_DIR

	// 批量获取
	if getResp, err = jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	// 初始化数组
	jobList = make([]*common.Job, 0)

	// 遍历所有任务，进行反序列化
	for _, kvPair = range getResp.Kvs {
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, &job); err != nil {
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}

	return
}

// 杀死任务
func (jobMgr *JobMgr) Kill(jobName string) (err error) {
	// 更新一下key=/cron/killer/ 任务名
	// 通知worker杀死任务
	var (
		killerKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId	clientv3.LeaseID
	)
	
	killerKey = common.JOB_KILLER_DIR + jobName

	// 让worker监听到put操作
	if leaseGrantResp,err = jobMgr.lease.Grant(context.TODO(), 1); err!=nil {
		return
	}

	// 租约ID
	leaseId = leaseGrantResp.ID

	// 设置killer标记
	if _,err = jobMgr.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err!=nil {
		return
	}

	return
}