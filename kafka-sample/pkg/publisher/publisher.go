package publisher

import (
	"context"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/m-mizutani/goerr"
)

// SimplePublisher は、sampleTopic に固定のメッセージを順に送信します。
// 設定値は基本的にデフォルト値を使用します。
// 配送結果は producer.Events() を購読する goroutine で受け取り、
// 最後に Flush で未送信メッセージを送り切ってから終了します。
func SimplePublisher(ctx context.Context) error {
	producer, err := kafka.NewProducer(
		&kafka.ConfigMap{
			"bootstrap.servers": bootstrapServers(),
		},
	)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	topic := "sampleTopic"

	// 配送結果(delivery report)を非同期に受け取る
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to %v: %s\n", ev.TopicPartition, string(ev.Value))
				}
			case kafka.Error:
				fmt.Printf("Error: %v\n", ev)
			default:
				fmt.Printf("Ignored event: %v\n", ev)
			}
		}
	}()

	messages := []string{"hello", "world", "foo", "bar", "baz"}
	for _, msg := range messages {
		select {
		case <-ctx.Done():
			fmt.Println("Context done, exiting...")
			return goerr.New("context done")
		default:
			// Produce は非同期。配送結果は上の Events() goroutine が受け取る。
			err := producer.Produce(
				&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Value:          []byte(msg),
				},
				nil,
			)
			if err != nil {
				fmt.Printf("Failed to produce message: %s\n", err)
				return goerr.New("failed to produce message")
			}
			fmt.Printf("Queued message: %s\n", msg)
		}
	}

	// 未送信のメッセージを送り切る(最大15秒待機)
	remaining := producer.Flush(15 * 1000)
	if remaining > 0 {
		fmt.Printf("Failed to flush all messages, %d remaining\n", remaining)
		return goerr.New("failed to flush all messages")
	}
	fmt.Println("All messages flushed")

	return nil
}
