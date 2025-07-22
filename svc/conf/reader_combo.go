package conf

import (
	"fmt"
)

type (
	comboReader struct {
		readers []Reader
	}
)

func (c *comboReader) Get(k string, dv ...string) (string, error) {
	var err error

	for _, r := range c.readers {
		v, e := r.Get(k)
		if e == nil && len(v) > 0 {
			return v, nil
		}
		if e != nil {
			if err == nil {
				err = e
			} else {
				err = fmt.Errorf("%s \n %w", err, e)
			}
		}
	}

	if len(dv) > 0 {
		return dv[0], err
	}
	return "", err
}

func (c *comboReader) GetAny(k string, target any) error {
	var err error

	for _, r := range c.readers {
		e := r.GetAny(k, target)
		if e == nil {
			return nil
		}
		if err == nil {
			err = e
		} else {
			err = fmt.Errorf("%s \n %w", err, e)
		}
	}
	return err
}
