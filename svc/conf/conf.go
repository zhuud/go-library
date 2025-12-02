package conf

import (
	"fmt"
	"log"

	"github.com/zhuud/go-library/svc/conf/internal"
)

type (
	// OptionFunc 配置选项函数类型
	OptionFunc func(*Conf)

	// Conf 内部配置选项
	Conf struct {
		FilePath string
	}
)

var (
	reader = internal.NewBasicReader()
)

func init() {
	setReader(newEnvReader())
}

// MustSetUp 设置配置，失败时 panic
// 使用 options 模式：MustSetUp(WithFilePath("path/to/config.yaml"))
func MustSetUp(opts ...OptionFunc) {
	err := SetUp(opts...)
	if err != nil {
		log.Fatalf("conf.MustSetUp config file path not set error: %v", err)
	}
}

// SetUp 设置配置
// 使用 options 模式：SetUp(WithFilePath("path/to/config.yaml"))
func SetUp(opts ...OptionFunc) error {
	var config Conf

	// 应用所有选项
	for _, opt := range opts {
		opt(&config)
	}

	tfr, err := newFileReader(withFilePath(config.FilePath))
	if err != nil {
		return fmt.Errorf("conf.SetUp.newFileReader error: %w", err)
	}

	AppendReader(tfr)
	return nil
}

func AppendReader(r Reader) {
	or := getReader()
	if or == nil {
		setReader(r)
		return
	}

	// 如果已经是 ComboReader，直接追加
	if ocr, ok := or.(*internal.ComboReader); ok {
		ocr.Readers = append(ocr.Readers, r)
		return
	}

	// 否则创建新的 ComboReader，保持原有优先级
	setReader(internal.NewComboReader([]Reader{or, r}))
}

func setReader(r Reader) {
	reader.Store(r)
}

func getReader() Reader {
	return reader.Load()
}

func Get(k string) (string, error) {
	return getReader().Get(k)
}

func GetUnmarshal(k string, target any) error {
	return getReader().GetAny(k, target)
}

// WithFilePath 设置配置文件路径
func WithFilePath(filePath string) OptionFunc {
	return func(options *Conf) {
		options.FilePath = filePath
	}
}
