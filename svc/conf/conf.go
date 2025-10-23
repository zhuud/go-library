package conf

import (
	"fmt"
	"log"
)

var (
	reader = new(basicReader)
)

func init() {
	_init()
	setReader(newEnvReader())
}

func MustSetUp(c Config) {
	err := SetUp(c)
	if err != nil {
		log.Fatalf("conf.MustSetUp config file path not set error %v", err)
	}
}

func SetUp(c Config) error {
	tfr, err := newFileReader(WithFile(c.FilePath))
	if err != nil {
		return fmt.Errorf("conf.SetUp.newFileReader error %w", err)
	}

	AppendReader(tfr)
	return nil
}

func AppendReader(r Reader) {
	or := reader.Load()
	if or == nil {
		setReader(r)
		return
	}

	// 如果已经是 comboReader，直接追加
	if ocr, ok := or.(*comboReader); ok {
		ocr.readers = append(ocr.readers, r)
		return
	}

	// 否则创建新的 comboReader，保持原有优先级
	setReader(&comboReader{
		readers: []Reader{or, r},
	})
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
