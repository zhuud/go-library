package conf

import (
	"os"
	"strings"

	"github.com/spf13/cast"
)

// 部署配置的环境变量（私有，通过函数访问）
var (
	podIp       string // 容器ip
	podName     string // 容器名
	env         string // 部署的环境：local/test/pre/prod
	appZone     string // 部署所在分区：bj2c/bj2g
	appBuildNum string // 编译版本号
	appTestNum  string // 测试环境分区
	pprofPort   int    // pprof端口
	httpPort    int    // http端口
	appLogPath  string // 日志目录
	appConfPath string // 配置目录
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
	podIp = getString(`POD_IP`)
	podName = getString(`POD_NAME`)

	env = getString(`APP_RUN_ENV`, `local`)
	appBuildNum = getString(`APP_BUILD_NUM`)
	appZone = getString(`APP_ZONE`, `bj2c`)
	nodeHost, err := os.ReadFile(`/etc/k8s_node_hostname`)
	if err == nil {
		tmp := strings.Split(string(nodeHost), `-`)
		if len(tmp) > 1 && tmp[0] == `al` && strings.HasPrefix(tmp[1], `bj2`) {
			appZone = tmp[1]
		}
	}
	appTestNum = getString(`APP_RUN_TEST_NUM`)
	pprofPort = getInt(`PPROF_PORT`, 9527)
	httpPort = getInt(`HTTP_PORT`)
	appLogPath = getString(`APP_LOG_PATH`)
	appConfPath = getString(`APP_CONFIG_PATH`)
}

// ===== 环境变量访问函数（只读） =====

// PodIp 获取容器 IP
func PodIp() string { return podIp }

// PodName 获取容器名
func PodName() string { return podName }

// Env 获取部署环境
func Env() string { return env }

// AppZone 获取部署所在分区
func AppZone() string { return appZone }

// AppBuildNum 获取编译版本号
func AppBuildNum() string { return appBuildNum }

// AppTestNum 获取测试环境分区
func AppTestNum() string { return appTestNum }

// PprofPort 获取 pprof 端口
func PprofPort() int { return pprofPort }

// HttpPort 获取 HTTP 端口
func HttpPort() int { return httpPort }

// AppLogPath 获取日志目录
func AppLogPath() string { return appLogPath }

// AppConfPath 获取配置目录
func AppConfPath() string { return appConfPath }

// ===== 环境判断函数 =====

// IsLocal 判断是否为本地环境
func IsLocal() bool {
	return env == EnvLocal
}

// IsProd 判断是否为生产环境
func IsProd() bool {
	return env == EnvProd
}

// IsPre 判断是否为预发布环境
func IsPre() bool {
	return env == EnvPre
}

// IsTest 判断是否为测试环境
func IsTest() bool {
	return env == EnvTest
}

func getString(k string, dv ...string) string {
	v := os.Getenv(k)
	if v == `` && len(dv) > 0 {
		return dv[0]
	}
	return v
}

func getInt(k string, dv ...int) int {
	v := os.Getenv(k)
	if v == `` {
		if len(dv) > 0 {
			return dv[0]
		}
		return 0
	}
	return cast.ToInt(v)
}
