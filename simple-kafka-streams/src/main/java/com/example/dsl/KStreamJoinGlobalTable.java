package com.example.dsl;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

@Slf4j
public class KStreamJoinGlobalTable {
    private final static String APPLICATION_NAME = "global-table-join-application";
    private final static String BOOTSTRAP_SERVERS = "devlinux:9092";
    private final static String ADDRESS_TABLE = "address_v2";
    private final static String ORDER_STREAM = "order";
    private final static String ORDER_JOIN_STREAM = "order_join";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        GlobalKTable<String, String> addressTable = streamsBuilder.globalTable(ADDRESS_TABLE);
        KStream<String, String> orderStream = streamsBuilder.stream(ORDER_STREAM);

        orderStream.join(
                addressTable, // global table 인스턴스
                (orderKey, orderValue) -> orderKey, // KTable 과 다르게 레코드를 매칭할 때 KStream 의 메시지 키와 값 사용 가능. 예를 들어, KStream 의 메시지 값을 GlobalKTable 의 메시지 키와 조인 가능
                (order, address)  -> order + " send to " + address)
                .to(ORDER_JOIN_STREAM);

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
    }
}
