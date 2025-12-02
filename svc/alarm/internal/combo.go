package internal

import (
	"errors"
	"fmt"
)

// ComboSender 组合发送器，支持多个发送器同时发送
type ComboSender struct {
	Senders []Sender
}

// NewComboSender 创建一个新的组合发送器
func NewComboSender(senders []Sender) *ComboSender {
	return &ComboSender{
		Senders: senders,
	}
}

func (c *ComboSender) Send(data any) error {
	var errs []error

	for i, s := range c.Senders {
		e := s.Send(data)
		if e != nil {
			errs = append(errs, fmt.Errorf("sender[%d]: %w", i, e))
		}
	}

	// 所有 sender 都要执行，收集所有错误
	if len(errs) > 0 {
		return fmt.Errorf("alarm.Send failed error/sender: %d/%d errors: %w", len(errs), len(c.Senders), errors.Join(errs...))
	}
	return nil
}
