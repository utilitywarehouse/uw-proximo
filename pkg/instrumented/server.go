package instrumented

import (
	"log"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/proto"
)

var errorsCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "errors_total",
	Help: "A counter of the number of errors",
})

func init() {
	prometheus.MustRegister(errorsCounter)
}

type sinkServer struct {
	delegate proto.MessageSinkServer
	debug    bool
}

func (s *sinkServer) Publish(ps proto.MessageSink_PublishServer) error {
	if err := s.delegate.Publish(ps); err != nil {
		errorsCounter.Inc()
		if s.debug {
			log.Printf("publish failed: %s\n", err.Error())
		}
		return err
	}
	return nil
}

// NewInstrumentedSinkServer returns a message sink server with error metrics.
func NewInstrumentedSinkServer(factory proximo.AsyncSinkFactory, debug bool) proto.MessageSinkServer {
	return &sinkServer{
		delegate: &proximo.SinkServer{
			SinkFactory: factory,
		},
		debug: debug,
	}
}

type sourceServer struct {
	delegate proto.MessageSourceServer
	debug    bool
}

func (s *sourceServer) Consume(cs proto.MessageSource_ConsumeServer) error {
	if err := s.delegate.Consume(cs); err != nil {
		errorsCounter.Inc()
		if s.debug {
			log.Printf("consume failed: %s\n", err.Error())
		}
		return err
	}
	return nil
}

// NewInstrumentedSourceServer returns a message source server with error metrics.
func NewInstrumentedSourceServer(factory proximo.AsyncSourceFactory, debug bool) proto.MessageSourceServer {
	return &sourceServer{
		delegate: &proximo.SourceServer{
			SourceFactory: factory,
			SkipDiscard:   debug,
		},
		debug: debug,
	}
}
