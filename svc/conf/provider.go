package conf

type Provider interface {
	Get(k string, def ...string) (string, error)
	GetUnmarshal(k string, target any) error
}
