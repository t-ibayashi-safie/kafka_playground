# kafka-sample

kafka を動かす最小構成のプログラムを配置する。

- `cmd/simple/publisher` … トピックに連番メッセージ(`message-N`)を一定間隔で送り続ける
- `cmd/simple/consumer` … トピックを購読してメッセージを表示する

## 設定(環境変数)

| 環境変数 | 説明 | デフォルト |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | 接続先ブローカー | `127.0.0.1:29092`(ホストから接続する想定) |
| `KAFKA_TOPIC` | 送信先 / 購読対象のトピック | `sampleTopic` |

## 実行

### コンテナ(リポジトリルートで実行)

compose では接続先は INTERNAL リスナーが設定済み。トピックは `KAFKA_TOPIC` で上書きする。

```sh
# 既定の sampleTopic に送信 / 購読
docker compose run --rm simple-publisher
docker compose run --rm simple-consumer

# 任意のトピックを対象にする(例: Kafka Streams の出力を確認する)
docker compose run --rm -e KAFKA_TOPIC=streamsUpperTopic simple-consumer
docker compose run --rm -e KAFKA_TOPIC=myTopic simple-publisher
```

### ホストから直接

```sh
go run ./cmd/simple/publisher
KAFKA_TOPIC=myTopic go run ./cmd/simple/consumer
```
