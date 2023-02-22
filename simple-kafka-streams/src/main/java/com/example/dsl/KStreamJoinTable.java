package com.example.dsl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class KStreamJoinTable {
    private final static String APPLICATION_NAME = "order-join-application";
    private final static String BOOTSTRAP_SERVERS = "devlinux:9092";
    private final static String ADDRESS_TABLE = "address";
    private final static String ORDER_STREAM = "order";
    private final static String ORDER_JOIN_STREAM = "order_join";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KTable<String, String> addressTable = streamsBuilder.table(ADDRESS_TABLE);
        KStream<String, String> orderStream = streamsBuilder.stream(ORDER_STREAM);

        orderStream.join(addressTable, (order, address)  -> order + " send to " + address).to(ORDER_JOIN_STREAM);

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
    }
}
