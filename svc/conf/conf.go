package conf

import (
	"fmt"
	"sync/atomic"

	"github.com/pkg/errors"
)

var (
	hasSet uint32
	reader = new(basicReader)
)

func init() {
	_init()

	setReader(newEnvReader())

	err := SetUp(Conf{FilePath: AppConfPath})
	if err != nil {
		fmt.Println(err.Error())
	}
}

func SetUp(c Conf) error {
	tfr, err := newFileReader(WithFile(c.FilePath))
	if err != nil {
		return errors.Wrap(err, "conf.SetUp.newFileReader error")
	}

	if len(c.FilePath) > 0 {
		atomic.StoreUint32(&hasSet, 1)
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
