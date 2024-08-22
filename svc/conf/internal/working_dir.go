package internal

import (
	"log"
	"os"
	"path/filepath"
	"strings"
)

var workingDir string

func WorkingDir() string {
	if workingDir == `` {
		workingDir, _ = os.Getwd()
		// 针对单元测试，循环向上找conf目录
		if (len(os.Args) >= 1 && strings.HasSuffix(os.Args[0], `.test`)) ||
			(len(os.Args) >= 2 && os.Args[1] == `-test.run`) {
			wd, _ := filepath.Abs(workingDir)
			log.Println("init wd: ", wd)
			for len(wd) > 1 {
				configFile := wd + `/etc/config.yaml`
				_, err := os.Stat(configFile)
				if !os.IsNotExist(err) {
					workingDir = wd
					break
				}
				wd = filepath.Dir(wd)
				log.Println("next wd: ", wd)
			}
		}
	}
	return workingDir
}
