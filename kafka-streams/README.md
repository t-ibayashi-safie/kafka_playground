# kafka-streams

Java の [Kafka Streams](https://kafka.apache.org/documentation/streams/) を使ったストリーム処理のサンプル。

入力トピック `sampleTopic`（`kafka-sample` の publisher が送信）を読み、2 種類の処理を行う。

| 処理 | 種別 | 内容 | 出力トピック |
|---|---|---|---|
| `mapValues` | ステートレス (KStream) | 値を大文字化 | `streamsUpperTopic` |
| `groupBy().count()` | ステートフル (KTable) | 値の `-` より前をキーに件数を集計 | `streamsCountTopic` |

実装は [src/main/java/com/example/streams/StreamsApp.java](src/main/java/com/example/streams/StreamsApp.java)。

## 起動方法

すべてリポジトリルートで実行する。

### 1. クラスタを起動

```sh
docker compose up -d broker-1 broker-2 broker-3
```

### 2. トピックを作成

入力 `sampleTopic` と、出力先の 2 トピックを作成する。
Kafka Streams は changelog / repartition などの内部トピックは自動生成するが、
`to()` の出力先トピックは自動生成しないため、事前に作成する。

```sh
for t in sampleTopic streamsUpperTopic streamsCountTopic; do
  docker compose run --rm kafka-cli \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server broker-1:19092 \
    --create --if-not-exists \
    --topic "$t" --partitions 3 --replication-factor 3
done
```

### 3. 入力メッセージを流す

別ターミナルで publisher を起動し、`sampleTopic` にメッセージを送り続ける。

```sh
docker compose run -e KAFKA_TOPIC=sampleTopic --rm simple-publisher
```

### 4. Streams アプリを起動

```sh
docker compose run --rm kafka-streams
```

起動時に処理トポロジが標準出力に表示され、処理ごとに `[upper]` / `[count]` のログが出る。

## 出力の確認

### simple-consumer で覗く

`kafka-sample` の consumer を `KAFKA_TOPIC` で出力トピックに向ければ手軽に確認できる。

```sh
docker compose run --rm -e KAFKA_TOPIC=streamsUpperTopic simple-consumer

docker compose run --rm -e KAFKA_TOPIC=streamsCountTopic simple-consumer
```

### kafka-cli で覗く

別ターミナルで出力トピックを覗く。

```sh
# 大文字化された値
docker compose run --rm kafka-cli \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker-1:19092 \
  --topic streamsUpperTopic --from-beginning

# キーごとの件数 (件数は文字列として出力しているのでそのまま読める)
docker compose run --rm kafka-cli \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker-1:19092 \
  --topic streamsCountTopic --from-beginning \
  --property print.key=true
```

## 構成メモ

- 接続先は環境変数 `KAFKA_BOOTSTRAP_SERVERS` で指定（compose では INTERNAL リスナー `broker-1:19092,...`）。未設定時はホストからの接続用 `127.0.0.1:29092`。
- Kafka Streams クライアントは `3.9.0`、Java 17。ビルドは Docker 内の Gradle で行うため、ホストに Gradle は不要。
