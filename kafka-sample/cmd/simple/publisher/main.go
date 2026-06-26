package main

import (
	"context"
	"fmt"

	"github.com/t-ibayashi-safie/kafka_playground/kafka-sample/pkg/publisher"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := publisher.SimplePublisher(ctx)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
