package conf

import (
	"github.com/spf13/cast"
	"os"
	"strings"
)

// 部署配置的一些环境变量
// TODO 使用方法获取
var (
	PodIp       string // 容器ip
	PodName     string // 容器名
	Env         string // 部署的环境：local/test/pre/prod
	AppZone     string // 部署所在分区：bj2c/bj2g
	AppBuildNum string // 编译版本号
	AppTestNum  string // 测试环境分区
	PprofPort   int    // pprof端口
	HttpPort    int    //http端口
	AppLogPath  string //日志目录
	AppConfPath string // 配置目录
)

const (
	EnvPre   = string(`pre`)
	EnvProd  = string(`prod`)
	EnvTest  = string(`test`)
	EnvLocal = string(`local`)
)

func init() {
	_init()
}

func _init() {
	PodIp = getString(`POD_IP`)
	PodName = getString(`POD_NAME`)

	Env = getString(`APP_RUN_ENV`, `local`)
	AppBuildNum = getString(`APP_BUILD_NUM`)
	AppZone = getString(`APP_ZONE`, `bj2c`)
	nodeHost, err := os.ReadFile(`/etc/k8s_node_hostname`)
	if err == nil {
		tmp := strings.Split(string(nodeHost), `-`)
		if len(tmp) > 1 && tmp[0] == `al` && strings.HasPrefix(tmp[1], `bj2`) {
			AppZone = tmp[1]
		}
	}
	AppTestNum = getString(`APP_RUN_TEST_NUM`)
	PprofPort = cast.ToInt(getString(`PPROF_PORT`, "9527"))
	HttpPort = getInt(`HTTP_PORT`)
	AppLogPath = getString(`APP_LOG_PATH`)
	AppConfPath = getString(`APP_CONFIG_PATH`)
}

func IsLocal() bool {
	return Env == EnvLocal
}
func IsProd() bool {
	return Env == EnvProd
}
func IsPre() bool {
	return Env == EnvPre
}
func IsTest() bool {
	return Env == EnvTest
}

func getString(k string, dv ...string) string {
	v := os.Getenv(k)
	if v == `` && len(dv) > 0 {
		return dv[0]
	}
	return v
}
func getInt(k string) int {
	v := os.Getenv(k)
	if v == `` {
		return 0
	}
	return cast.ToInt(v)
}
