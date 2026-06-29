# Gradle で distribution(bin + lib)をビルドする
FROM gradle:8.10-jdk17 AS builder

WORKDIR /app

# 依存解決を先に行いレイヤキャッシュを効かせる
COPY settings.gradle build.gradle ./
RUN gradle --no-daemon dependencies > /dev/null 2>&1 || true

COPY src ./src

# build/install/kafka-streams-app/{bin,lib} を生成する
RUN gradle --no-daemon installDist

FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=builder /app/build/install/kafka-streams-app ./

ENTRYPOINT ["./bin/kafka-streams-app"]
