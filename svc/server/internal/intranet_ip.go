//go:build !nps
// +build !nps

package internal

import (
	"fmt"
	"net"

	"github.com/zhuud/go-library/svc/conf"
)

func GetRegisterAddr() (string, error) {
	k8sip, _ := conf.Get("K8S_HOST_IP")
	if len(k8sip) > 0 {
		return fmt.Sprintf("%s:%d", k8sip, conf.PprofPort()), nil
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		// 检查ip地址判断是否回环地址
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return fmt.Sprintf("%s:%d", ipNet.IP.String(), conf.PprofPort()), nil
			}
		}
	}
	return "", fmt.Errorf("server.GetRegisterAddr cannot get ip")
}
