# kafka-cli

kafka-cliによる操作のサンプル

```sh
BootstrapServer="localhost:29092"
```

トピックの作成

```sh
./kafka/bin/kafka-topics.sh --create \
--bootstrap-server ${BootstrapServer} --replication-factor 1 --partitions 3 --topic "sampletopic"
```

トピック一覧

```sh
./kafka/bin/kafka-topics.sh --list \
  --bootstrap-server ${BootstrapServer}
```


トピックへメッセージを送信

```sh
./kafka/bin/kafka-console-producer.sh --bootstrap-server ${BootstrapServer} --topic "sampletopic"
```

メッセージを購読

```sh
./kafka/bin/kafka-console-consumer.sh \
--bootstrap-server ${BootstrapServer} --topic "sampletopic" --group testgroup --from-beginning

./kafka/bin/kafka-console-consumer.sh \
--bootstrap-server ${BootstrapServer} --topic "sampletopic" --group testgroup
```
