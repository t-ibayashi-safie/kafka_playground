package com.example.streams;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

/**
 * sampleTopic を入力に、Kafka Streams の 2 種類の処理をデモするアプリ。
 *
 * <ol>
 *   <li>ステートレス変換(KStream): 値を大文字化して streamsUpperTopic へ出力</li>
 *   <li>ステートフル集計(KTable): "message-0" などの "-" より前をキーとして
 *       件数をカウントし、streamsCountTopic へ出力</li>
 * </ol>
 */
public class StreamsApp {

    public static void main(String[] args) {
        String bootstrapServers =
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092");

        Properties props = new Properties();
        // 同一 application.id を持つインスタンス群で 1 つの consumer group を形成する
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-sample");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // コミット済みオフセットがない場合は最初から読む
        props.put(
                StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // 入力: sampleTopic を KStream(追記型のレコードストリーム)として読む
        KStream<String, String> source =
                builder.stream("sampleTopic", Consumed.with(Serdes.String(), Serdes.String()));

        // 1) ステートレス変換: 値を大文字化して別トピックへ
        source.mapValues(value -> value.toUpperCase())
                .peek((key, value) -> System.out.println("[upper] " + value))
                .to("streamsUpperTopic", Produced.with(Serdes.String(), Serdes.String()));

        // 2) ステートフル集計: "-" より前をキーにして件数をカウント(結果は KTable)
        source.groupBy(
                        (key, value) -> {
                            int idx = value.indexOf('-');
                            return idx > 0 ? value.substring(0, idx) : value;
                        },
                        Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("message-counts"))
                .toStream()
                .peek((key, count) -> System.out.println("[count] " + key + " => " + count))
                // 件数(Long)を文字列にしてから出力する。
                // Long のまま出力するとバイナリになり、simple-consumer など
                // 値を文字列として表示するコンシューマーでは文字化けするため。
                .mapValues(count -> Long.toString(count))
                .to("streamsCountTopic", Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();
        // トポロジ(処理グラフ)を起動時に表示する
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);

        // Ctrl-C / SIGTERM で正常終了する
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    streams.close();
                                    latch.countDown();
                                }));

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.err.println("Streams app failed: " + e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }
}
