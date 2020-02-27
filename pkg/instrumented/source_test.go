package instrumented

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/uw-labs/substrate"
)

type asyncMessageSourceMock struct {
	substrate.AsyncMessageSource
	consumerMessagesMock func(context.Context, chan<- substrate.Message, <-chan substrate.Message) error
}

func (m asyncMessageSourceMock) ConsumeMessages(ctx context.Context, in chan<- substrate.Message, acks <-chan substrate.Message) error {
	return m.consumerMessagesMock(ctx, in, acks)
}

type Message struct {
	data []byte
}

func (m Message) Data() []byte {
	return m.data
}

func TestConsumeMessagesSuccessfully(t *testing.T) {
	receivedAcks := make(chan substrate.Message)

	source := AsyncMessageSource{
		impl: &asyncMessageSourceMock{
			consumerMessagesMock: func(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
				messages <- Message{}

				for {
					select {
					case <-ctx.Done():
						return nil
					case ack := <-acks:
						receivedAcks <- ack
					}
				}
			},
		},
		counter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Help: "source_counter",
				Name: "source_counter",
			}, sourceLabels),
		topic:    "testTopic",
		consumer: "testConsumer",
	}

	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message)

	sourceContext, sourceCancel := context.WithCancel(context.Background())
	defer sourceCancel()

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- source.ConsumeMessages(sourceContext, messages, acks)
	}()

	for {
		select {
		case m := <-messages:
			acks <- m
		case err := <-errs:
			assert.NoError(t, err)
			return
		case <-receivedAcks:
			var metric dto.Metric
			assert.NoError(t, source.counter.WithLabelValues("success", "testTopic", "testConsumer").Write(&metric))
			assert.Equal(t, 1, int(*metric.Counter.Value))

			sourceCancel()
		}
	}
}

func TestConsumeMessagesWithError(t *testing.T) {
	consumingErr := errors.New("consuming error")

	source := AsyncMessageSource{
		impl: &asyncMessageSourceMock{
			consumerMessagesMock: func(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
				return consumingErr
			},
		},
		counter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Help: "source_counter",
				Name: "source_counter",
			}, sourceLabels),
		topic:    "testErrorTopic",
		consumer: "testErrorConsumer",
	}

	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message, 1)

	sourceContext, sourceCancel := context.WithCancel(context.Background())
	defer sourceCancel()

	errs := make(chan error)
	go func() {
		defer close(errs)
		errs <- source.ConsumeMessages(sourceContext, messages, acks)
	}()

	err := <-errs
	assert.Error(t, err)
	assert.Equal(t, consumingErr, err)

	var metric dto.Metric
	assert.NoError(t, source.counter.WithLabelValues("error", "testErrorTopic", "testErrorConsumer").Write(&metric))
	assert.Equal(t, 1, int(*metric.Counter.Value))

	sourceCancel()
}
