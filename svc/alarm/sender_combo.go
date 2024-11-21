package alarm

import (
	"github.com/pkg/errors"
)

type (
	comboSender struct {
		senders []Sender
	}
)

func (c *comboSender) Send(data any) (err error) {
	for _, r := range c.senders {
		e := r.Send(data)
		if e != nil {
			if err == nil {
				err = e
			} else {
				err = errors.Wrap(e, err.Error())
			}
		}
	}
	return err
}
