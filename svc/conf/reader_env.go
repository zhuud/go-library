package conf

import (
	"encoding/json"

	"github.com/pkg/errors"
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

func (r *envReader) Get(k string, dv ...string) (string, error) {
	return getString(k, dv...), nil
}

func (r *envReader) GetAny(k string, target any) error {
	if len(k) == 0 {
		return errors.New("conf.envReader k empty")
	}

	v := getString(k)
	if len(v) == 0 {
		return errors.New("conf.envReader.Get nil")
	}

	err := json.Unmarshal([]byte(v), target)
	return errors.Wrap(err, "conf.envReader.GetAny error")
}
