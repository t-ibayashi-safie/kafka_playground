package consumer

import "os"

// bootstrapServers は、接続先の Kafka ブローカーを返します。
// 環境変数 KAFKA_BOOTSTRAP_SERVERS が設定されていればそれを使用し、
// 未設定の場合はホストから接続する想定のデフォルト値を返します。
func bootstrapServers() string {
	if v := os.Getenv("KAFKA_BOOTSTRAP_SERVERS"); v != "" {
		return v
	}
	return "127.0.0.1:29092"
}

// topic は、購読対象のトピックを返します。
// 環境変数 KAFKA_TOPIC が設定されていればそれを使用し、
// 未設定の場合はデフォルトの sampleTopic を返します。
func topic() string {
	if v := os.Getenv("KAFKA_TOPIC"); v != "" {
		return v
	}
	return "sampleTopic"
}
