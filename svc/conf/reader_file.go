package conf

import (
	"encoding/json"
	"fmt"
	"github.com/mitchellh/mapstructure"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zhuud/go-library/svc/conf/internal"
)

type fileReader struct {
	handler *viper.Viper
}

// file
func newFileReader(c Conf) (Reader, error) {
	if fr != nil {
		return fr, nil
	}
	rmu.Lock()
	defer rmu.Unlock()

	opts := append([]OptionFunc{}, WithFile(c.FilePath))
	handleOptions(opts)

	if len(options.FilePath) == 0 {
		if len(AppConfigPath) > 0 {
			options.FilePath = AppConfigPath
		} else {
			options.FilePath = fmt.Sprintf(`%s/etc/config.%s.yaml`, internal.WorkingDir(), Env)
		}
	}
	viper.SetConfigFile(options.FilePath)
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}

	return &fileReader{
		handler: viper.GetViper(),
	}, nil
}

func (r *fileReader) Get(k string, dv ...string) (string, error) {
	v := r.handler.GetString(k)
	if len(v) == 0 && len(dv) > 0 {
		return dv[0], nil
	}
	return v, nil
}

func (r *fileReader) GetAny(k string, target any) error {
	if len(k) == 0 {
		err := conf.Load(options.FilePath, target)
		return errors.Wrap(err, "conf.fileReader.Load error")
	}

	v := r.handler.Get(k)
	if v == nil {
		return errors.New("conf.fileReader.Get nil")
	}

	if sv, ok := v.(string); ok {
		err := json.Unmarshal([]byte(sv), target)
		return errors.Wrap(err, "conf.fileReader.Unmarshal error")
	}

	err := mapstructure.WeakDecode(v, target)
	return errors.Wrap(err, "conf.fileReader.WeakDecode error")
}
