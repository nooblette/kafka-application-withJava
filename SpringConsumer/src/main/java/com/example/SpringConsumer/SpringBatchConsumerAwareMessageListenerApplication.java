package com.example.SpringConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

@SpringBootApplication
public class SpringBatchConsumerAwareMessageListenerApplication {
    public static Logger logger = LoggerFactory.getLogger(SpringBatchConsumerAwareMessageListenerApplication.class);

    public static void main(String[] args){
        SpringApplication application = new SpringApplication(SpringBatchConsumerAwareMessageListenerApplication.class);
        application.run(args);
    }

    @KafkaListener(topics = "test", groupId = "test-group-01")
    public void commitListener(ConsumerRecords<String, String> records,
                               Acknowledgment ack){ // ack-mode가 manual 또는 manual_immediate 일 경우 수동커밋을 하기위해 Acknowledgment 인스턴스를 파라미터로 받음
        records.forEach(record -> logger.info(record.toString()));
        ack.acknowledge(); // 커밋을 수행
    }

    @KafkaListener(topics = "test", groupId = "test-group-02")
    public void consumerCommitListener(ConsumerRecords<String, String> records,
                                       Consumer<String, String> consumer){ // 동기, 비동기 커밋을 사용하기 위해 Consumer 인스턴스를 파라미터로 받음
        records.forEach(record -> logger.info(record.toString()));
        consumer.commitAsync(); //consumer의 commitSync() 또는 commitAsync() 메소드를 호출하여 사용자가 원하는 타이밍에 커밋
    }
}
