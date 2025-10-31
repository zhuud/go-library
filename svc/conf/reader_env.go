package conf

import (
	"encoding/json"
	"fmt"
)

type envReader struct {
}

var (
	er *envReader
)

// env
func newEnvReader() Reader {
	if er != nil {
		return er
	}
	rmu.Lock()
	defer rmu.Unlock()
	return &envReader{}
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
