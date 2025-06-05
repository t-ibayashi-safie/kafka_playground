package consumer

import (
	"context"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/m-mizutani/goerr"
)

// RetrySeekConsumer は、メッセージを読み込み、表示します。
// 処理が失敗した場合、再度メッセージを読み込み、リトライします。
// seekを使用して、失敗したメッセージのオフセットをリトライします。
// NOTE:
// - Seekを利用したリトライは、リトライ対象以外のメッセージの処理を遅らせるなど、非効率
// - commitのタイミングなどによって、順序保証が崩れる場合があるらしい。
func RetrySeekConsumer(ctx context.Context) error {
	consumer, err := kafka.NewConsumer(
		&kafka.ConfigMap{
			"bootstrap.servers": "127.0.0.1:29092",
			"group.id":          "RetryConsumer1Group",
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
	errInterval := 3
	processMessage := func(e *kafka.Message) error {
		// メッセージの処理を行う
		// 一定の条件でエラーを返す
		cnt++
		if cnt%errInterval == 0 {
			return goerr.New("processing error")
		}

		return nil
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
				// メッセージの処理を行う
				err := processMessage(e)

				if err != nil {
					// 処理に失敗した場合、Seekを使用して、失敗したメッセージのオフセットをSeekし、次回のPollで再度読み込む
					fmt.Printf("Processing error for message on %s: %s\n", e.TopicPartition, string(e.Value))
					err = consumer.Seek(e.TopicPartition, 0)
					if err != nil {
						fmt.Printf("Failed to seek to offset: %s\n", err)
						return goerr.New("failed to seek to offset")
					}
					fmt.Printf("Seeking to offset: %s\n", e.TopicPartition)
					continue
				}
				fmt.Printf("Processed message on %s: %s\n", e.TopicPartition, string(e.Value))

				// 手動でオフセットをコミット
				commitOffsets := []kafka.TopicPartition{e.TopicPartition}
				_, err = consumer.CommitOffsets(commitOffsets)
				if err != nil {
					fmt.Printf("Failed to commit offsets: %s\n", err)
					return goerr.New("failed to commit offsets")
				}
				fmt.Printf("Committed offsets: %v\n", commitOffsets)
				continue
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
