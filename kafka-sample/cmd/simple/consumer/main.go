package main

import (
	"context"
	"fmt"

	"github.com/t-ibayashi-safie/kafka_playground/kafka-sample/pkg/consumer"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := consumer.SimpleConsumer(ctx)
	// err := consumer.ManualCommitConsumer(ctx)
	// err := consumer.RetryAsyncConsumer(ctx)
	// err := consumer.RetrySeekConsumer(ctx)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
