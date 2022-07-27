package com.example.SpringConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.List;

@SpringBootApplication
public class SpringBatchMessageListenerConsumerApplication {
    public static Logger logger = LoggerFactory.getLogger(SpringBatchMessageListenerConsumerApplication.class);

    public static void main(String[] args){
        SpringApplication application =
                new SpringApplication(SpringBatchMessageListenerConsumerApplication.class);
        application.run(args);
    }

    /* KafkaListener 어노테이션으로 사용되는 메서드의 파라미터를 List 혹은 ConsumerRecords 로 받음 */
    @KafkaListener(topics = "test",
            groupId = "test-group-01")
    public void batchListener(ConsumerRecords<String, String> records){
        records.forEach(record -> logger.info(record.toString()));
    }


    @KafkaListener(topics = "test",
            groupId = "test-group-02")
    public void batchListener(List<String> list){
        list.forEach(recordValue -> logger.info(recordValue));
    }


    @KafkaListener(topics = "test",
            groupId = "test-group-03",
            concurrency = "3") // 3개의 컨뮤서 스레드를 생성
    public void concurrentBatchListener(ConsumerRecords<String, String> records){
        records.forEach(record -> logger.info(record.toString()));
    }
}
