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

func MustSetUp(c Conf) {
	err := SetUp(c)
	if err != nil {
		log.Fatalf("conf.MustSetUp config file path not set error %v", err)
	}
}

func SetUp(c Conf) error {
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
	} else {
		ocr, ok := or.(*comboReader)
		if ok {
			ocr.readers = append(ocr.readers, r)
		} else {
			ocr = &comboReader{
				readers: []Reader{r, or},
			}
		}
		setReader(ocr)
	}
}

func setReader(r Reader) {
	reader.Store(r)
}

func getReader() Reader {
	return reader.Load()
}

func Get(k string, dv ...string) (string, error) {
	return getReader().Get(k, dv...)
}

func GetUnmarshal(k string, target any) error {
	return getReader().GetAny(k, target)
}
