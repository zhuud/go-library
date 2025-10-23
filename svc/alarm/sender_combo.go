package alarm

import (
	"errors"
	"fmt"
)

type (
	comboSender struct {
		senders []Sender
	}
)

func (c *comboSender) Send(data any) error {
	var errs []error

	for i, s := range c.senders {
		e := s.Send(data)
		if e != nil {
			errs = append(errs, fmt.Errorf("sender[%d]: %w", i, e))
		}
	}

	// 所有 sender 都要执行，收集所有错误
	if len(errs) > 0 {
		return fmt.Errorf("alarm.Send failed %d/%d senders: %w", len(errs), len(c.senders), errors.Join(errs...))
	}
	return nil
}
