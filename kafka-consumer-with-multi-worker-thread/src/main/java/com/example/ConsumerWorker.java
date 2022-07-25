package com.example;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWorker implements Runnable{
    private final static Logger logger = LoggerFactory.getLogger(ConsumerMultiWorker.class);
    private Properties prop;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;

    ConsumerWorker(Properties prop, String topic, int number) { // KafkaConsumer 인스터 생성자
        this.prop = prop;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;
    }

    @Override
    public void run() {
        consumer = new KafkaConsumer<>(prop); // KafkaConsumer 클래스는 스레드 세이프 하지않아, 스레드 별로 Kafka Consumer 인스턴스를 별개로 생성하여 운영
        consumer.subscribe(Arrays.asList(topic)); // 생성자 변수로 받은 토픽을 명시적으로 구독
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord<String, String> record : records){
                logger.info("{}", record); // poll 메소드를 통해 리턴받은 레코드를  처리
            }
        }
    }
}
