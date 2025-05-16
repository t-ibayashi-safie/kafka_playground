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
			"bootstrap.servers": "127.0.0.1:29092",
			"auto.offset.reset": "earliest",
			// このグループに対して以前にコミットされたオフセットがない場合、
			// 割り当てられた各パーティションの最初のメッセージから読み込みを開始する。
			"group.id":                        "SimpleConsumerGroup",
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

// ManualCommitConsumer は、メッセージを読み込み、表示します。
// 手動でコミットを行うための設定を使用します。
func ManualCommitConsumer(ctx context.Context) error {
	consumer, err := kafka.NewConsumer(
		&kafka.ConfigMap{
			"bootstrap.servers": "127.0.0.1:29092",
			"group.id":          "ManualCommitConsumerGroup",
			// このグループに対して以前にコミットされたオフセットがない場合、
			// 割り当てられた各パーティションの最初のメッセージから読み込みを開始する。
			"auto.offset.reset":               "earliest",
			"go.application.rebalance.enable": true, // 再バランシングを有効化

			// コミットされたオフセットを自動的に保存しない
			"enable.auto.offset.store": false,
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

	cnt := 0
	commitInterval := 5 // 5メッセージごとにコミット
	commitOffsets := []kafka.TopicPartition{}
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
				cnt++
				if cnt%commitInterval == 0 {
					// 手動でオフセットをコミット
					commitOffsets = append(commitOffsets, e.TopicPartition)
					_, err := consumer.CommitOffsets(commitOffsets)
					if err != nil {
						fmt.Printf("Failed to commit offsets: %s\n", err)
						return goerr.New("failed to commit offsets")
					}
					fmt.Printf("Committed offsets: %v\n", commitOffsets)
					commitOffsets = []kafka.TopicPartition{} // リセット
				}
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
