package worker

import (
	"encoding/json"
	"io/ioutil"
)

// 程序配置
type Config struct {
	EtcdEndpoints   []string `json:"etcdEndpoints"`
	EtcdDialTime    int      `json:"etcdDialTimeout"`
}

// 单例
var G_config *Config

func InitConfig(filename string) (err error) {
	// 读配置文件
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}

	// json反序列化

	conf := &Config{}

	if err = json.Unmarshal(content, conf); err != nil {
		return
	}

	// 赋值单例
	G_config = conf

	return
}
