package db

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "fdbx"
	subsystem = "db"

	opConnect = "c"
	opRead    = "r"
	opWrite   = "w"
)

// All Prometheus collectors from this module
var (
	OpsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "ops_total",
		Help:      "Number of (c)onnect/(r)ead/(w)rite physical transactions",
	}, []string{"op"})

	CallsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "calls_total",
		Help:      "Number of (c)onnect/(r)ead/(w)rite method calls",
	}, []string{"op"})

	ErrorsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "errors_total",
		Help:      "Number of (c)onnect/(r)ead/(w)rite errors",
	}, []string{"op"})

	CallsHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "calls_seconds",
		Help:      "Response time of (c)onnect/(r)ead/(w)rite method calls",
	}, []string{"op"})
)

//goland:noinspection GoUnusedGlobalVariable
var Metrics = []prometheus.Collector{
	OpsCounter,
	CallsCounter,
	ErrorsCounter,
	CallsHistogram,
}
