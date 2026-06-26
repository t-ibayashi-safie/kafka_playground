# confluent-kafka-go は cgo + バンドル librdkafka を使うため、
# ビルドには C コンパイラ(glibc 環境)が必要。
FROM golang:1.24-bookworm AS builder

WORKDIR /app

# 依存解決を先に行いレイヤキャッシュを効かせる
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# consumer / publisher の両方をビルドする
RUN CGO_ENABLED=1 go build -o /simple-consumer ./cmd/simple/consumer \
	&& CGO_ENABLED=1 go build -o /simple-publisher ./cmd/simple/publisher

FROM debian:bookworm-slim

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=builder /simple-consumer /usr/local/bin/simple-consumer
COPY --from=builder /simple-publisher /usr/local/bin/simple-publisher

# 起動するバイナリは compose の command で選択する(既定は consumer)
CMD ["simple-consumer"]
