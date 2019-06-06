package instrumented

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/instrumented"
)

// AsyncSinkFactory adds metrics and health check to every new sink.
type AsyncSinkFactory struct {
	BackendStatusChecker BackendStatusChecker
	CounterOpts          prometheus.CounterOpts
	SinkFactory          proximo.AsyncSinkFactory
}

func (f AsyncSinkFactory) NewAsyncSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	sink, err := f.SinkFactory.NewAsyncSink(ctx, req)
	if err != nil {
		return nil, err
	}
	sink = instrumentedAsyncSink{
		statusChecker: f.BackendStatusChecker,
		connID:        fmt.Sprintf("%s-%s", req.GetTopic(), generateID()),
		sink:          sink,
	}

	return instrumented.NewAsyncMessageSink(sink, f.CounterOpts, req.GetTopic()), nil
}

// AsyncSourceFactory adds metrics and health check to every new sink.
type AsyncSourceFactory struct {
	BackendStatusChecker BackendStatusChecker
	CounterOpts          prometheus.CounterOpts
	SourceFactory        proximo.AsyncSourceFactory
}

func (f AsyncSourceFactory) NewAsyncSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	source, err := f.SourceFactory.NewAsyncSource(ctx, req)
	if err != nil {
		return nil, err
	}
	source = instrumentedAsyncSource{
		statusChecker: f.BackendStatusChecker,
		connID:        fmt.Sprintf("%s-%s-%s", req.GetTopic(), req.GetConsumer(), generateID()),
		source:        source,
	}

	return instrumented.NewAsyncMessageSource(source, f.CounterOpts, req.GetTopic()), nil
}

type instrumentedAsyncSink struct {
	statusChecker BackendStatusChecker
	connID        string
	sink          substrate.AsyncMessageSink
}

func (s instrumentedAsyncSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.statusChecker.RegisterStatuser(s.connID, s.sink, cancel)
	return s.sink.PublishMessages(ctx, acks, messages)
}

func (s instrumentedAsyncSink) Close() error {
	s.statusChecker.RemoveStatuser(s.connID)
	return s.sink.Close()
}

func (s instrumentedAsyncSink) Status() (*substrate.Status, error) {
	return s.sink.Status()
}

type instrumentedAsyncSource struct {
	statusChecker BackendStatusChecker
	connID        string
	source        substrate.AsyncMessageSource
}

func (s instrumentedAsyncSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.statusChecker.RegisterStatuser(s.connID, s.source, cancel)
	return s.source.ConsumeMessages(ctx, messages, acks)
}

func (s instrumentedAsyncSource) Close() error {
	s.statusChecker.RemoveStatuser(s.connID)
	return s.source.Close()
}

func (s instrumentedAsyncSource) Status() (*substrate.Status, error) {
	return s.source.Status()
}
