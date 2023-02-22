package com.example.dsl;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

@Slf4j
public class SimpleStreamApplication {
    private final static String APPLICATION_NAME = "stream-application";
    private final static String BOOTSTRAP_SERVERS = "devlinux:9092";
    private final static String STREAM_LOG = "stream_log";
    private final static String STREAM_LOG_COPY = "stream_log_copy";

    public static void main(String[] args) {
        Properties properties = new Properties();
        // stream 은 아이디 값을 기준으로 병렬 처리
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        // stream 애플리케이션과 연동할 카프타 클러스터 정보
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 스트림 토폴로지를 정의하기 위한 클래스
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // stream_log 란 토픽으로부터 KStream 객체를 만들기 위해 stream 메서드 사용
        // KTable 은 table(), GlobalKTable 은 globalTable() 사용
        // 이 메서드들은 최초의 토픽 데이터를 가져오는 소스 프로세서
        KStream<String, String> streamLog = streamsBuilder.stream(STREAM_LOG);
        // stream_log 토픽을 담은 KStream 객체를 다른 토픽으로 전송하기 위해 to() 메서드 사용
        // to 메서드는 데이터들을 특정 토픽으로 저장하기 위한 용도로 사용되었다. 즉, to() 메서드는 싱크 프로세서다
        streamLog.to(STREAM_LOG_COPY);

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
    }
}
