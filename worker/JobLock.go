package worker

import (
	"context"

	"github.com/yeongbok77/crontab/common"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 分布式锁（txn事务）
type JobLock struct {
	kv clientv3.KV
	lease clientv3.Lease
	
	jobName string	// 任务名
	cancleFunc context.CancelFunc	// 用于终止自动续租
	leaseId clientv3.LeaseID
	isLocked bool	
}

// 初始化一把锁
func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease)  (jobLock *JobLock) {
	jobLock = &JobLock{
		kv: kv,
		lease: lease,
		jobName: jobName,
	}
	return
}

// 尝试上锁
func (jobLock *JobLock) TryLock() (err error) {
	var (
		txn clientv3.Txn
		keepResp *clientv3.LeaseKeepAliveResponse
		lockKey string
		txnResp *clientv3.TxnResponse
	)
	// 创建租约
	leaseGrantResp,err := jobLock.lease.Grant(context.TODO(), 5)
	if err!=nil {
		return
	}
	
	cancelCtx, cancelFunc := context.WithCancel(context.TODO())

	// 租约id
	leaseId := leaseGrantResp.ID

	// 自动续租
	keepRespChan,err :=  jobLock.lease.KeepAlive(cancelCtx, leaseId)
	if err!=nil {
		goto FAIL
	}

	// 处理续租应答的协程
	go func ()  {
		for {
			select{
			case keepResp = <- keepRespChan:	// 自动续租的应答
				if keepResp == nil {
					goto END
				}
			}
		}
		END:
	}()

	// 创建事务txn
	txn = jobLock.kv.Txn(context.TODO())

	//锁路径
	lockKey = common.JOB_LOCK_DIR + jobLock.jobName

	// 事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))

	txnResp,err = txn.Commit()
	if err!=nil {
		goto FAIL
	}

	// 成功，失败释放租约
	if !txnResp.Succeeded {	//锁被占用
		err = common.ERR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}
	
	// 抢锁成功
	jobLock.leaseId = leaseId
	jobLock.cancleFunc = cancelFunc
	jobLock.isLocked = true
	return


FAIL:
	cancelFunc()
	jobLock.lease.Revoke(context.TODO(), leaseId)	// 释放租约
	return	
}

func (jobLock *JobLock) Unlock() {
	if jobLock.isLocked == true{
		jobLock.cancleFunc()
	jobLock.lease.Revoke(context.TODO(), jobLock.leaseId)
	}
	
}