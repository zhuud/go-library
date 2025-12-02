package internal

import (
	"errors"
	"fmt"
)

// ComboReader 组合读取器，支持多个读取器按优先级读取
type ComboReader struct {
	Readers []Reader
}

// NewComboReader 创建一个新的组合读取器
func NewComboReader(readers []Reader) *ComboReader {
	return &ComboReader{
		Readers: readers,
	}
}

func (c *ComboReader) Get(k string) (string, error) {
	var errs []error

	for i, r := range c.Readers {
		v, e := r.Get(k)
		if e == nil && len(v) > 0 {
			return v, nil
		}
		if e != nil {
			errs = append(errs, fmt.Errorf("reader[%d]: %w", i, e))
		}
	}

	if len(errs) > 0 {
		return "", fmt.Errorf("conf.Get failed from all readers key: %s, errors: %w", k, errors.Join(errs...))
	}
	return "", nil
}

func (c *ComboReader) GetAny(k string, target any) error {
	var errs []error

	for i, r := range c.Readers {
		e := r.GetAny(k, target)
		if e == nil {
			return nil
		}
		errs = append(errs, fmt.Errorf("reader[%d]: %w", i, e))
	}

	if len(errs) > 0 {
		return fmt.Errorf("conf.GetAny failed from all readers key: %s, errors: %w", k, errors.Join(errs...))
	}
	return nil
}

func (c *ComboReader) Name() string {
	return "combo"
}
