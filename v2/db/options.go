package db

// Option - метод для перегрузки некоторых свойств подключения
type Option func(*options) error

type options struct {
	ClusterFile string
	WithMetrics bool
}

// ClusterFile - нестандартный путь до кластер-файла FoundationDB
func ClusterFile(name string) Option {
	return func(o *options) error {
		// TODO: проверка файла
		o.ClusterFile = name
		return nil
	}
}

// WithMetrics - enable prometheus measurements (disabled by default)
func WithMetrics() Option {
	return func(o *options) error {
		o.WithMetrics = true
		return nil
	}
}
