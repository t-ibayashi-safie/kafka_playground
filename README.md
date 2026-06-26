

```sh
# トピック一覧
sudo docker compose run --rm kafka-cli \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BROKER:9098 \
  --command-config /client/client-iam.properties \
  --list
```
