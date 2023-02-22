package com.example.dsl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class FilterStreamer {
    private final static String APPLICATION_NAME = "stream-filter-application";
    private final static String BOOTSTRAP_SERVERS = "devlinux:9092";
    private final static String STREAM_LOG = "stream_log";
    private final static String STREAM_LOG_COPY = "stream_log_filter";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> streamLog = streamsBuilder.stream(STREAM_LOG);
        // stream filter 추가
        KStream<String, String> filteredStream = streamLog.filter(((key, value) -> value.length() > 5));
        filteredStream.to(STREAM_LOG_COPY);

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
    }
}
