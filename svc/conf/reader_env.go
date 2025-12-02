package conf

import (
	"encoding/json"
	"fmt"
	"sync"
)

type envReader struct {
}

var (
	er   *envReader
	erMu sync.Mutex
)

// env
func newEnvReader() Reader {
	if er != nil {
		return er
	}
	erMu.Lock()
	defer erMu.Unlock()

	// 双重检查
	if er != nil {
		return er
	}

	er = &envReader{}
	return er
}

func (r *envReader) Get(k string) (string, error) {
	return getString(k), nil
}

func (r *envReader) GetAny(k string, target any) error {
	if len(k) == 0 {
		return fmt.Errorf("conf.envReader k empty")
	}

	v := getString(k)
	if len(v) == 0 {
		return fmt.Errorf("conf.envReader.Get nil")
	}

	err := json.Unmarshal([]byte(v), target)
	if err != nil {
		return fmt.Errorf("conf.envReader.GetAny error: %w", err)
	}
	return nil
}

func (r *envReader) Name() string {
	return "env"
}
