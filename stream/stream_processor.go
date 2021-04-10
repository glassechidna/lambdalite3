package stream

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"time"
)

type Processor struct {
}

func (p *Processor) Handle(ctx context.Context, input *events.DynamoDBEvent) error {
	return nil
}

type DynamoStreamEvent struct {
	events.DynamoDBEvent
	ShardId                 string `json:"shardId"`
	EventSourceARN          string `json:"eventSourceARN"`
	State                   json.RawMessage `json:"state"`
	IsFinalInvokeForWindow  bool `json:"isFinalInvokeForWindow"`
	IsWindowTerminatedEarly bool `json:"IsWindowTerminatedEarly"`
	Window struct {
		Start time.Time `json:"start"`
		End time.Time `json:"end"`
	} `json:"window"`
}
