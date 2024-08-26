package conf

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
)

type (
	Reader interface {
		Get(k string, dv ...string) (string, error)
		GetAny(k string, target any) error
	}

	basicReader struct {
		reader Reader
		mu     sync.RWMutex
	}

	comboReader struct {
		readers []Reader
	}
)

var (
	er *envReader
	fr *fileReader

	rmu sync.RWMutex
)

func (br *basicReader) Load() Reader {
	br.mu.RLock()
	defer br.mu.RUnlock()
	return br.reader
}

func (br *basicReader) Store(v Reader) Reader {
	br.mu.Lock()
	defer br.mu.Unlock()
	br.reader = v
	return br.reader
}

func (c *comboReader) Get(k string, dv ...string) (string, error) {
	var msg string
	var err error

	for _, r := range c.readers {
		v, e := r.Get(k)
		if e == nil && len(v) > 0 {
			return v, nil
		}
		if e != nil {
			msg = fmt.Sprintf("%s \n %s", msg, e.Error())
		}
	}

	if len(msg) > 0 {
		err = errors.New(msg)
	}
	if len(dv) > 0 {
		return dv[0], err
	}
	return "", err
}

func (c *comboReader) GetAny(k string, target any) error {
	var msg string

	for _, r := range c.readers {
		err := r.GetAny(k, target)
		if err == nil {
			return nil
		}
		msg = fmt.Sprintf("%s \n %s", msg, err.Error())
	}
	return errors.New(msg)
}
