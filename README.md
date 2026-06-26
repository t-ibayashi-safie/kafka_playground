
## kafka Clusterの起動

```sh
docker compose up -d broker-1 broker-2 broker-3
```


## kafka-cliによる捜査

```sh
BROKER=broker-1:19092

# トピック一覧
docker compose run --rm kafka-cli \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BROKER \
  --list

# トピック作成
docker compose run --rm kafka-cli \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BROKER \
  --create \
  --topic sample-topic \
  --partitions 3 \
  --replication-factor 3
```

## producer の起動

```sh
docker compose run --rm kafka-sample \
/kafka-sample
```


## consumer の起動
