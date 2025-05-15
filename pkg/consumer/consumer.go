package consumer

import (
	"context"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/m-mizutani/goerr"
)

// RunSimpleConsumer は、メッセージを読み込み、表示します。
// 設定値は基本的にデフォルト値を使用します。
func RunSimpleConsumer(ctx context.Context) error {
	consumer, err := kafka.NewConsumer(
		&kafka.ConfigMap{
			"bootstrap.servers":               "127.0.0.1:29092",
			"group.id":                        "myGroup",
			"go.application.rebalance.enable": true, // 再バランシングを有効化
		},
	)
	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{"sampleTopic"}, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe to topics: %s\n", err)
		return goerr.New("failed to subscribe to topics")
	}

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context done, exiting...")
			return goerr.New("context done")
		default:
			event := consumer.Poll(0)
			switch e := event.(type) {
			case *kafka.Message:
				fmt.Printf("Message on %s: %s\n", e.TopicPartition, string(e.Value))
			case kafka.Error:
				if e.IsFatal() {
					fmt.Printf("Fatal error: %v\n", e)
					return goerr.New("fatal error")
				} else {
					fmt.Printf("Error: %v\n", e)
				}
			case kafka.PartitionEOF:
				fmt.Printf("End of partition event: %v\n", e)
			case kafka.OffsetsCommitted:
				fmt.Printf("Offsets committed: %v\n", e)
			case nil:
				// No event
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}
}
