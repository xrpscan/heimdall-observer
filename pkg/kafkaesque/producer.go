package kafkaesque

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Producer represents a generic Kafka Producer.
type Producer interface {
	Produce(ctx context.Context, payload []byte, headers map[string]string) error
	Close(ctx context.Context) error
}

// FranzGoProducer implements [Producer] using github.com/twmb/franz-go.
type FranzGoProducer struct {
	client *kgo.Client
}

// ProducerParams contain params required to create a Kafka Producer.
type ProducerParams struct {
	Brokers    []string
	Username   string
	Password   string
	CACertPath string
	Topic      string

	Logger Logger
}

// NewFranzGoProducer returns a new FranzGoProducer instance.
func NewFranzGoProducer(ctx context.Context, params ProducerParams) (*FranzGoProducer, error) {
	// Logger is optional.
	if params.Logger == nil {
		params.Logger = noopLogger{}
	}

	// Common config.
	opts := []kgo.Opt{
		kgo.SeedBrokers(params.Brokers...),
		kgo.DefaultProduceTopic(params.Topic),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
		// franz-go already has sensible defaults for configs like RetryCount and RetryBackoff.
	}

	// Enable SCRAM-SHA and TLS if username and password provided.
	if params.Username != "" && params.Password != "" {
		params.Logger.Info("username and password are present, SCRAM-SHA and TLS will be enabled")

		// Add SCRAM-SHA option.
		mechanism := scram.Auth{User: params.Username, Pass: params.Password}.AsSha512Mechanism()
		opts = append(opts, kgo.SASL(mechanism))

		// Add TLS option.
		dialer, err := createTLSDialer(params.CACertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS dialer: %w", err)
		}

		opts = append(opts, kgo.Dialer(dialer.DialContext))
	} else {
		params.Logger.Info("username and password are absent, SCRAM-SHA and TLS will be disabled")
	}

	// Create connection.
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	// Verify connection.
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("failed to ping kafka cluster: %w", err)
	}

	return &FranzGoProducer{client: cl}, nil
}

func (f *FranzGoProducer) Produce(
	ctx context.Context, payload []byte, headers map[string]string,
) error {
	// Form message.
	record := kgo.SliceRecord(payload)
	for key, value := range headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}

	// Produce.
	if err := f.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	return nil
}

func (f *FranzGoProducer) Close(_ context.Context) error {
	f.client.Close()
	return nil
}
