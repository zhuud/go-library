package alarm

type Provider interface {
	Send(data any) error
}
