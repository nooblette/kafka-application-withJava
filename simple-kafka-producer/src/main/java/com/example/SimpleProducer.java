package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class SimpleProducer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    private static void setKafkaProducerKeyNull(KafkaProducer<String, String> producer, Properties configs){
        String messageValue = "testMessage";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        producer.send(record);
        logger.info("{}", record);
        producer.flush();
        producer.close();
    }

    private static void setKafkaProducerKeyNotNull(KafkaProducer<String, String> producer, Properties configs){
        String messageKey = "Pangyo";
        String messageValue = "23";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageKey, messageValue);
        producer.send(record);
        logger.info("{}", record);
        producer.flush();
        producer.close();
    }

    private static void setKafkaProducerPartitionNo(KafkaProducer<String, String> producer, Properties configs){
        int partitionNo = 0;
        String messageKey = "Pangyo";
        String messageValue = "23";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partitionNo, messageKey, messageValue);
        producer.send(record);
        logger.info("{}", record);
        producer.flush();
        producer.close();
    }

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        /* 실습용, 예제에 맞게 주석을 해제하며 실행 */
        //setKafkaProducerKeyNull(producer, configs); // 메시지 키가 없는 데이터를 전송
        //setKafkaProducerKeyNotNull(producer, configs); // 메시지 키를 갖는 데이터를 전송
        setKafkaProducerPartitionNo(producer, configs); // 파티션 번호를 지정하여 데이터를 전송
    }
}
