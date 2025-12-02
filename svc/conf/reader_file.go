package conf

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zhuud/go-library/svc/conf/internal"
)

type (
	// fileOptionFunc defines the method to customize the file reader.
	fileOptionFunc func(config *fileConf)

	fileConf struct {
		FilePath string
	}

	fileReader struct {
		handler *viper.Viper
	}
)

var (
	fr   *fileReader
	frMu sync.Mutex
)

// file
func newFileReader(opts ...fileOptionFunc) (Reader, error) {
	if fr != nil {
		return fr, nil
	}
	frMu.Lock()
	defer frMu.Unlock()

	// 双重检查
	if fr != nil {
		return fr, nil
	}

	var config fileConf
	for _, opt := range opts {
		opt(&config)
	}

	if len(config.FilePath) == 0 {
		wd := internal.WorkingDir()
		config.FilePath = fmt.Sprintf(`%s/etc/config.%s.yaml`, wd, Env())
	}

	viper.SetConfigFile(config.FilePath)
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}

	fr = &fileReader{
		handler: viper.GetViper(),
	}
	return fr, nil
}

func withFilePath(filePath string) fileOptionFunc {
	return func(options *fileConf) {
		options.FilePath = filePath
	}
}

func (r *fileReader) Get(k string) (string, error) {
	return r.handler.GetString(k), nil
}

func (r *fileReader) GetAny(k string, target any) error {
	if len(k) == 0 {
		err := conf.Load(r.handler.ConfigFileUsed(), target)
		if err != nil {
			return fmt.Errorf("conf.fileReader.Load error: %w", err)
		}
		return nil
	}

	v := r.handler.Get(k)
	if v == nil {
		return fmt.Errorf("conf.fileReader.Get nil")
	}

	if sv, ok := v.(string); ok {
		err := json.Unmarshal([]byte(sv), target)
		if err != nil {
			return fmt.Errorf("conf.fileReader.Unmarshal error: %w", err)
		}
		return nil
	}

	err := mapstructure.WeakDecode(v, target)
	if err != nil {
		return fmt.Errorf("conf.fileReader.WeakDecode error: %w", err)
	}
	return nil
}

func (r *fileReader) Name() string {
	return "file"
}
