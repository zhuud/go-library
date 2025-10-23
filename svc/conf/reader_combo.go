package conf

import (
	"errors"
	"fmt"
)

type (
	comboReader struct {
		readers []Reader
	}
)

func (c *comboReader) Get(k string) (string, error) {
	var errs []error

	for i, r := range c.readers {
		v, e := r.Get(k)
		if e == nil && len(v) > 0 {
			return v, nil
		}
		if e != nil {
			errs = append(errs, fmt.Errorf("reader[%d]: %w", i, e))
		}
	}

	if len(errs) > 0 {
		return "", fmt.Errorf("conf.Get key %s failed from all readers: %w", k, errors.Join(errs...))
	}
	return "", nil
}

func (c *comboReader) GetAny(k string, target any) error {
	var errs []error

	for i, r := range c.readers {
		e := r.GetAny(k, target)
		if e == nil {
			return nil
		}
		errs = append(errs, fmt.Errorf("reader[%d]: %w", i, e))
	}

	if len(errs) > 0 {
		return fmt.Errorf("conf.GetAny key=%s failed from all readers: %w", k, errors.Join(errs...))
	}
	return nil
}
