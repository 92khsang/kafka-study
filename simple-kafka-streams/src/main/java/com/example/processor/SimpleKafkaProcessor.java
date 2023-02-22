package com.example.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

@Slf4j
public class SimpleKafkaProcessor {
    private final static String APPLICATION_NAME = "processor-application";
    private final static String BOOTSTRAP_SERVERS = "devlinux:9092";

    private final static String STREAM_LOG = "stream_log";

    private final static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Process API 를 사용하여 토폴로지를 구성하기 위해 사용
        Topology topology = new Topology();
        topology.addSource("Source", STREAM_LOG) // 소스 프로세서를 가져오기 위해 addSource 메서드 사용. 첫 번째 파라미터에는 소스 프로세서의 이름을 입력하고 두 번쨰 파라미터는 대상 토픽 이름을 입력
                .addProcessor("Process", () -> new FilterProcessor(), "Source") // 스트림 프로세서슬 사용하기 위해 addProcessor 메서드 사용
                .addSink("Sink", STREAM_LOG_FILTER, "Process"); // 싱크 프로세서로서 데이터를 저장하기 위해 addSink 메서드를 사용

        KafkaStreams streaming = new KafkaStreams(topology, properties);
        streaming.start();
    }
}
