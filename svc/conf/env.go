package conf

import (
	"os"
	"sync"

	"github.com/spf13/cast"
)

const (
	EnvPre   = string(`pre`)
	EnvProd  = string(`prod`)
	EnvTest  = string(`test`)
	EnvLocal = string(`local`)
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

	envInitOnce sync.Once // 确保环境变量只初始化一次
)

// initEnv 初始化环境变量，必须在其他初始化之前调用
func initEnv() {
	podIp = getString(`POD_IP`)
	podName = getString(`POD_NAME`)

	env = getString(`APP_RUN_ENV`, `local`)
	appBuildNum = getString(`APP_BUILD_NUM`)
	appZone = getString(`APP_ZONE`, `bj2c`)
	appTestNum = getString(`APP_RUN_TEST_NUM`)
	pprofPort = getInt(`PPROF_PORT`, 9527)
	httpPort = getInt(`HTTP_PORT`)

	appLogPath = getString(`APP_LOG_PATH`)
	appConfPath = getString(`APP_CONFIG_PATH`)
}

// initEnvOnce 初始化环境变量（仅执行一次，线程安全）
func initEnvOnce() {
	envInitOnce.Do(initEnv)
}

// PodIp 获取容器 IP
// 注意：可以安全地在任何地方调用，包括包级别变量初始化时
func PodIp() string {
	initEnvOnce()
	return podIp
}

// PodName 获取容器名
func PodName() string {
	initEnvOnce()
	return podName
}

// Env 获取部署环境
func Env() string {
	initEnvOnce()
	return env
}

// AppZone 获取部署所在分区
func AppZone() string {
	initEnvOnce()
	return appZone
}

// AppBuildNum 获取编译版本号
func AppBuildNum() string {
	initEnvOnce()
	return appBuildNum
}

// AppTestNum 获取测试环境分区
func AppTestNum() string {
	initEnvOnce()
	return appTestNum
}

// PprofPort 获取 pprof 端口
func PprofPort() int {
	initEnvOnce()
	return pprofPort
}

// HttpPort 获取 HTTP 端口
func HttpPort() int {
	initEnvOnce()
	return httpPort
}

// AppLogPath 获取日志目录
func AppLogPath() string {
	initEnvOnce()
	return appLogPath
}

// AppConfPath 获取配置目录
func AppConfPath() string {
	initEnvOnce()
	return appConfPath
}

// ===== 环境判断函数 =====

// IsLocal 判断是否为本地环境
func IsLocal() bool {
	initEnvOnce()
	return env == EnvLocal
}

// IsProd 判断是否为生产环境
func IsProd() bool {
	initEnvOnce()
	return env == EnvProd
}

// IsPre 判断是否为预发布环境
func IsPre() bool {
	initEnvOnce()
	return env == EnvPre
}

// IsTest 判断是否为测试环境
func IsTest() bool {
	initEnvOnce()
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
