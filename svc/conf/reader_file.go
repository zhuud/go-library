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
	// FileOptionFunc defines the method to customize the logging.
	FileOptionFunc func(option *fileOption)
	fileOption     struct {
		FilePath string
	}

	fileReader struct {
		handler *viper.Viper
	}
)

var (
	fileOptions fileOption
	fr          *fileReader
)

// file
func newFileReader(opts ...FileOptionFunc) (Reader, error) {
	if fr != nil {
		return fr, nil
	}
	rmu.Lock()
	defer rmu.Unlock()

	handleOptions(opts)

	if len(fileOptions.FilePath) == 0 {
		fileOptions.FilePath = fmt.Sprintf(`%s/etc/config.%s.yaml`, internal.WorkingDir(), Env)
	}

	viper.SetConfigFile(fileOptions.FilePath)
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
		opt(&fileOptions)
	}
}

func WithFile(filePath string) FileOptionFunc {
	return func(opts *fileOption) {
		opts.FilePath = filePath
	}
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
		err := conf.Load(fileOptions.FilePath, target)
		return fmt.Errorf("conf.fileReader.Load error %w", err)
	}

	v := r.handler.Get(k)
	if v == nil {
		return fmt.Errorf("conf.fileReader.Get nil")
	}

	if sv, ok := v.(string); ok {
		err := json.Unmarshal([]byte(sv), target)
		return fmt.Errorf("conf.fileReader.Unmarshal error %w", err)
	}

	err := mapstructure.WeakDecode(v, target)
	return fmt.Errorf("conf.fileReader.WeakDecode error %w", err)
}
