package consumer

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/m-mizutani/goerr"
)

// RetryAsyncConsumer は、メッセージを読み込み、表示します。
// 処理が失敗した場合、固定回数のリトライを行い、それでもダメな場合は、リトライキューにメッセージを送信します。
// リトライキューは、別のコンシューマーで非同期に処理します。
// それでもダメな場合は、デッドレターキューにメッセージを送信します。
func RetryAsyncConsumer(ctx context.Context) error {
	ctx2, cancel := context.WithCancel(ctx)
	go func() {
		err := retryAsyncConsumer_main(ctx2)
		if err != nil {
			fmt.Printf("Error in retryConsumer2_main: %v\n", err)
			cancel()
		}
	}()

	go func() {
		err := retryAsyncConsumer_sub(ctx2)
		if err != nil {
			fmt.Printf("Error in retryConsumer2_sub: %v\n", err)
			cancel()
		}
	}()
	// Wait for the context to be done
	<-ctx.Done()
	fmt.Println("Context done, exiting...")
	return goerr.New("context done")
}

// retryAsyncConsumer_main は、メッセージを読み込み、表示します。
// 処理が失敗した場合、固定回数のリトライを行い、それでもダメな場合は、デッドレターキューにメッセージを送信します。
func retryAsyncConsumer_main(ctx context.Context) error {

	mainTopic := "sampleTopic"
	retryTopic := "retryTopic"
	// deadLetterTopic := "deadLetterTopic"

	// メッセージを受信するconsumer
	consumer, err := kafka.NewConsumer(
		&kafka.ConfigMap{
			"bootstrap.servers":               "127.0.0.1:29092",
			"group.id":                        "RetryConsumer2Group",
			"auto.offset.reset":               "latest",
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

	// リトライーキューにメッセージを送信するproducer
	producer, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "127.0.0.1:29092"})
	defer producer.Close()

	err = consumer.SubscribeTopics([]string{mainTopic}, nil)
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
					// 処理に失敗した場合、リトライキューにメッセージを送信
					fmt.Printf("Send message to retryTopic on %s: %s\n", e.TopicPartition, string(e.Value))
					deliveryChan := make(chan kafka.Event)
					err = producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &retryTopic, Partition: kafka.PartitionAny},
						Value:          e.Value,
					}, deliveryChan)

					if err != nil {
						fmt.Printf("Failed to send message to retryTopic: %s\n", err)
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

// retryAsyncConsumer_sub は、リトライキューを処理します。
// 失敗した場合は、デッドレターキューにメッセージを送信します。
func retryAsyncConsumer_sub(ctx context.Context) error {
	retryTopic := "retryTopic"
	deadLetterTopic := "deadLetterTopic"

	// リトライキューを処理するconsumer
	consumer, err := kafka.NewConsumer(
		&kafka.ConfigMap{
			"bootstrap.servers": "127.0.0.1:29092",
			"group.id":          "ManualCommitConsumerGroup",
			// このグループに対して以前にコミットされたオフセットがない場合、
			// 割り当てられた各パーティションの最初のメッセージから読み込みを開始する。
			"auto.offset.reset":               "latest",
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
	producer, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "127.0.0.1:29092"})
	defer producer.Close()

	err = consumer.SubscribeTopics([]string{retryTopic}, nil)
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
					fmt.Printf("Failed to process message on %s: %s\n", e.TopicPartition, string(e.Value))

					err := producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &deadLetterTopic, Partition: kafka.PartitionAny},
						Value:          e.Value,
					}, nil)
					if err != nil {
						fmt.Printf("Failed to send message to deadLetterTopic: %s\n", err)
					} else {
						fmt.Printf("Send message to deadLetterTopic on %s: %s\n", e.TopicPartition, string(e.Value))
					}
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
