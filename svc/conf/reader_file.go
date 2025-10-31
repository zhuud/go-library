package conf

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zhuud/go-library/svc/conf/internal"
)

type (
	// FileOptionFunc defines the method to customize the file reader.
	FileOptionFunc func(config *fileConfig)

	fileConfig struct {
		FilePath string
	}

	fileReader struct {
		handler *viper.Viper
	}
)

var (
	fc fileConfig
	fr *fileReader
)

// file
func newFileReader(opts ...FileOptionFunc) (Reader, error) {
	if fr != nil {
		return fr, nil
	}
	rmu.Lock()
	defer rmu.Unlock()

	handleOptions(opts)

	if len(fc.FilePath) == 0 {
		wd := internal.WorkingDir()
		fc.FilePath = fmt.Sprintf(`%s/etc/config.%s.yaml`, wd, Env())
	}

	viper.SetConfigFile(fc.FilePath)
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}

	return &fileReader{
		handler: viper.GetViper(),
	}, nil
}

func handleOptions(opts []FileOptionFunc) {
	for _, opt := range opts {
		opt(&fc)
	}
}

func WithFile(filePath string) FileOptionFunc {
	return func(config *fileConfig) {
		config.FilePath = filePath
	}
}

func (r *fileReader) Get(k string) (string, error) {
	return r.handler.GetString(k), nil
}

func (r *fileReader) GetAny(k string, target any) error {
	if len(k) == 0 {
		err := conf.Load(fc.FilePath, target)
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
