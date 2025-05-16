package consumer

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/m-mizutani/goerr"
)

// SimpleConsumer は、メッセージを読み込み、表示します。
// 設定値は基本的にデフォルト値を使用します。
func SimpleConsumer(ctx context.Context) error {
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

// RetryConsumer1 は、メッセージを読み込み、表示します。
// 処理が失敗した場合、再度メッセージを読み込み、リトライします。
// seekを使用して、失敗したメッセージのオフセットをリトライします。
// NOTE:
// - Seekを利用したリトライは、リトライ対象以外のメッセージの処理を遅らせるなど、非効率
// - commitのタイミングなどによって、順序保証が崩れる場合があるらしい。
func RetryConsumer1(ctx context.Context) error {
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

func RetryConsumer2(ctx context.Context) error {
	ctx2, cancel := context.WithCancel(ctx)
	go func() {
		err := retryConsumer2_A(ctx2)
		if err != nil {
			fmt.Printf("Error in retryConsumer2_A: %v\n", err)
			cancel()
		}
	}()

	go func() {
		err := retryConsumer2_B(ctx2)
		if err != nil {
			fmt.Printf("Error in retryConsumer2_B: %v\n", err)
			cancel()
		}
	}()
	// Wait for the context to be done
	<-ctx.Done()
	fmt.Println("Context done, exiting...")
	return goerr.New("context done")
}

// RetryConsumer2_A は、メッセージを読み込み、表示します。
// 処理が失敗した場合、固定回数のリトライを行い、それでもダメな場合は、デッドレターキューにメッセージを送信します。
func retryConsumer2_A(ctx context.Context) error {
	// メッセージを受信するconsumer
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

	// デッドレターキューにメッセージを送信するproducer
	deadLetterTopic := "deadLetterTopic"
	producer, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "127.0.0.1:29092"})
	defer producer.Close()

	err = consumer.SubscribeTopics([]string{"sampleTopic"}, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe to topics: %s\n", err)
		return goerr.New("failed to subscribe to topics")
	}

	processMessage := func(e *kafka.Message) error {
		if string(e.Value) == "error" {
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
				retryCount := 0
				maxRetry := 3
				success := false
				for retryCount < maxRetry {
					// メッセージの処理を行う
					fmt.Printf("Processing message on %s: %s, retryCount: %d\n", e.TopicPartition, string(e.Value), retryCount)
					err = processMessage(e)
					if err == nil {
						success = true
						break
					}
					retryCount++
					time.Sleep(time.Duration(retryCount) * 500 * time.Millisecond)
				}

				if !success {
					// 処理に失敗した場合、デッドレターキューにメッセージを送信
					fmt.Printf("Send message to dead letter topic on %s: %s\n", e.TopicPartition, string(e.Value))
					deliveryChan := make(chan kafka.Event)
					err = producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &deadLetterTopic, Partition: kafka.PartitionAny},
						Value:          e.Value,
					}, deliveryChan)

					if err != nil {
						fmt.Printf("Failed to send message to dead letter topic: %s\n", err)
					} else {
						ev := <-deliveryChan
						m := ev.(*kafka.Message)
						if m.TopicPartition.Error != nil {
							fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
						} else {
							fmt.Printf("Message delivered to %v\n", m.TopicPartition)
						}
					}
					close(deliveryChan)
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

// RetryConsumer2_B は、デッドレターキューを処理する
func retryConsumer2_B(ctx context.Context) error {
	deadLetterTopic := "deadLetterTopic"
	// デッドレターキューを処理するconsumer
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

	err = consumer.SubscribeTopics([]string{deadLetterTopic}, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe to topics: %s\n", err)
		return goerr.New("failed to subscribe to topics")
	}

	processMessage := func(e *kafka.Message) error {
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
				retryCount := 0
				maxRetry := 3
				success := false
				for retryCount < maxRetry {
					// メッセージの処理を行う
					fmt.Printf("Processing message on %s: %s, retryCount: %d\n", e.TopicPartition, string(e.Value), retryCount)
					err = processMessage(e)
					if err == nil {
						success = true
						break
					}
					retryCount++
					time.Sleep(time.Duration(retryCount) * 500 * time.Millisecond)
				}

				if !success {
					// 処理に失敗した場合
					// アラートなど上げる
					fmt.Printf("Failed to process message on %s: %s\n", e.TopicPartition, string(e.Value))
				}

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
