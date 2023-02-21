package com.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class SyncOffsetShutdownHookConsumer {
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "devlinux:9092";
    private final static String GROUP_ID = "test-group";

    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        // Consumer 어플리케이션은 안전하게 종료되어야 함
        // 정상적으로 종료되지 않은 컨슈머는 세션 타임아웃이 발생할때가지 컨슈머 그룹에 남음
        // 이로 인해 실제로 종료되었지만 더는 동작하지 않는 컨슈머가 존재하기 떄문에 파티션의 데이터는 소모되지 않고 랙이 늘어난다
        // 컨슈머 랙이 늘어나면 데이터 처리 지연이 발생
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("{}", record);
                }
            }
        } catch (WakeupException e) {
            log.error("Wakeup consumer");
        } finally {
            consumer.close();
        }
    }

    // 사용자는 안전한 종료를 위해 셧다운 훅을 등록
    static class ShutdownThread extends Thread {
        @Override
        public void run() {
            log.info("Shutdown hook");
            if (consumer != null) {
                // consumer 의 안전한 종료를 위한 매서드
                consumer.wakeup();
            }
        }
    }
}
