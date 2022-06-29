package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

public class RebalanceListenerConsumer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";
    public static KafkaConsumer<String, String> consumer;
    public static Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    public static void main(String[] args){
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 리밸런스 발생시 수동 커밋을 하기 위함

        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener()); // ConsumerRebalanceListner 인터페스로 구현한 RebalanceListner를 subscribe() 메소드에 오버라이드 변수로 포함

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord<String, String> record : records){
                logger.info("{}", record);
                currentOffsets.put(new TopicPartition(record.topic(), record, partition()), new OffsetAndMetadata(record.offset() + 1), null);
                consumer.commitSync(currentOffsets);
            }
        }
    }

    private static class RebalanceListener implements ConsumerRebalanceListener{ // 리밸런싱 발생을 감지하는 카프카 라이브러리
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) { // 리밸런스가 시작되기 직전에 호출되는 메소드
            logger.warn("Partitions are revoked");
            consumer.commitSync(currentOffsets); // 리밸런싱 직전에 커밋을 수행
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) { // 리밸런스가 끝난 뒤에 파티션이 할당되면 호출되는 메소드
            logger.warn("Partitions are assigned");
        }
    }
}
