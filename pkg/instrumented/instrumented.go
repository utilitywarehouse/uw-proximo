package instrumented

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/utilitywarehouse/go-operational/op"

	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/instrumented"
)

// AsyncSinkFactory adds metrics and health check to every new sink.
type AsyncSinkFactory struct {
	OpStatus    *op.Status
	CounterOpts prometheus.CounterOpts
	SinkFactory proximo.AsyncSinkFactory
}

func (f AsyncSinkFactory) NewAsyncSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	sink, err := f.SinkFactory.NewAsyncSink(ctx, req)
	if err != nil {
		return nil, err
	}
	sink = instrumentedAsyncSink{
		opStatus: f.OpStatus,
		hcName:   fmt.Sprintf("%s-%s", req.GetTopic(), generateID()),
		sink:     sink,
	}

	return instrumented.NewAsyncMessageSink(sink, f.CounterOpts, req.GetTopic()), nil
}

// AsyncSourceFactory adds metrics and health check to every new sink.
type AsyncSourceFactory struct {
	OpStatus      *op.Status
	CounterOpts   prometheus.CounterOpts
	SourceFactory proximo.AsyncSourceFactory
}

func (f AsyncSourceFactory) NewAsyncSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	source, err := f.SourceFactory.NewAsyncSource(ctx, req)
	if err != nil {
		return nil, err
	}
	source = instrumentedAsyncSource{
		opStatus: f.OpStatus,
		hcName:   fmt.Sprintf("%s-%s-%s", req.GetTopic(), req.GetConsumer(), generateID()),
		source:   source,
	}

	return instrumented.NewAsyncMessageSource(source, f.CounterOpts, req.GetTopic()), nil
}

type instrumentedAsyncSink struct {
	opStatus *op.Status
	hcName   string
	sink     substrate.AsyncMessageSink
}

func (s instrumentedAsyncSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
	s.opStatus.AddChecker(s.hcName, newOpsCheck(s.sink, "Can't publish messages."))
	return s.sink.PublishMessages(ctx, acks, messages)
}

func (s instrumentedAsyncSink) Close() error {
	s.opStatus.RemoveCheckers(s.hcName)
	return s.sink.Close()
}

func (s instrumentedAsyncSink) Status() (*substrate.Status, error) {
	return s.sink.Status()
}

type instrumentedAsyncSource struct {
	opStatus *op.Status
	hcName   string
	source   substrate.AsyncMessageSource
}

func (s instrumentedAsyncSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	s.opStatus.AddChecker(s.hcName, newOpsCheck(s.source, "Can't consume messages."))
	return s.source.ConsumeMessages(ctx, messages, acks)
}

func (s instrumentedAsyncSource) Close() error {
	s.opStatus.RemoveCheckers(s.hcName)
	return s.source.Close()
}

func (s instrumentedAsyncSource) Status() (*substrate.Status, error) {
	return s.source.Status()
}
