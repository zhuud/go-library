package conf

import (
	"fmt"
	"sync/atomic"

	"github.com/go-zookeeper/zk"
	"github.com/pkg/errors"
)

type (

	// OptionFunc defines the method to customize the logging.
	OptionFunc func(option *option)
	option     struct {
		FilePath      string
		ZookeeperConn *zk.Conn
	}
)

var (
	hasSet  uint32
	options option
	reader  = new(basicReader)
)

func init() {
	_init()
	_, err := SetUp(Conf{})
	if err != nil {
		fmt.Println(err.Error())
	}
}

func SetUp(c Conf) (Reader, error) {
	r := setReader(newEnvReader())
	tfr, err := newFileReader(c)
	if err != nil {
		err = errors.Wrap(err, "conf.SetUp.newFileReader error")
	} else {
		r = AppendReader(tfr)
		if len(c.FilePath) > 0 {
			atomic.StoreUint32(&hasSet, 1)
		}
	}
	return r, err
}

func AppendReader(r Reader) Reader {
	or := reader.Load()
	if or == nil {
		return setReader(r)
	} else {
		ocr, ok := or.(*comboReader)
		if ok {
			ocr.readers = append(ocr.readers, r)
		} else {
			ocr = &comboReader{
				readers: []Reader{r, or},
			}
		}
		return setReader(ocr)
	}
}

func setReader(r Reader) Reader {
	return reader.Store(r)
}

func getReader() Reader {
	return reader.Load()
}

func WithFile(filePath string) OptionFunc {
	return func(opts *option) {
		opts.FilePath = filePath
	}
}

func WithZookeeper(conn *zk.Conn) OptionFunc {
	return func(opts *option) {
		opts.ZookeeperConn = conn
	}
}

func handleOptions(opts []OptionFunc) {
	for _, opt := range opts {
		opt(&options)
	}
}

func Get(k string, dv ...string) (string, error) {
	return getReader().Get(k, dv...)
}

func GetUnmarshal(k string, target any) error {
	return getReader().GetAny(k, target)
}
