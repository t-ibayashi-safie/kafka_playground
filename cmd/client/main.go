package main

import (
	"context"
	"fmt"

	"github.com/t-ibayashi-safie/kafka_playground/pkg/consumer"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := consumer.RunSimpleConsumer(ctx)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
