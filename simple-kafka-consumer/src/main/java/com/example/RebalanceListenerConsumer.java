package com.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

// 컨슈머가 추가 또는 제거되면 파티션을 컨슈머에 재할당하는 과정인 리벨런스가 일어낟나
// poll 메서드를 통해 반환받은 데이터를 모두 처리하기 전에 리벨런스가 발생하면 데이터 중복이 발생할 수 있다.
//  - 데이터 일부를 처리했으나 커밋하지 않았기 때문
// 리벨런스 발생 시 데이터를 중복 처리하지 않기 위해 리벨런스 발생 시 처리한 데이털르 기준으로 커밋을 시도해야 한다

@Slf4j
public class RebalanceListenerConsumer {
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "devlinux:9092";
    private final static String GROUP_ID = "test-group";
    private static KafkaConsumer<String, String> consumer;
    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // commit 이 자동으로 일어나지 못하도록하는 설정

        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener());

        currentOffsets = new HashMap<>();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                log.info("{}", record);
                currentOffsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, null)
                );

                consumer.commitSync(currentOffsets);
            }
        }
    }

    // 리벨런스를 감지하기 위한 인터페이스 ConsumerRebalanceListener
    private static class RebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.warn("Partitions are assigned");
        } // 리벨런스가 끝날 뒤에 파티션이 할당 완료되면 호출되는 메서드

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.warn("Partitions are revoked");
            consumer.commitSync(currentOffsets);
        } // 리벨런스가 시작되기 직전에 호출되는 메서드
    }
}
