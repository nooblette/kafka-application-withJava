package com.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CustomPartitioner implements Partitioner {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if(keyBytes == null){
            throw new InvalidRecordException("Need message key");
        }
        if(((String)key).equals("Pangyo")){
            return 0;
        }

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 커스텀 파티셔너를 지정할 경우 PARTITION_CLASS_CONFIG 옵션을 통해 사용자 생성 파티셔너로 설정하여 KafkaProducer 인스턴스를 생성
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String messageKey = "Pangyo";
        String messageValue = "23";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageKey, messageValue);

        // send() 메서드는 브로커로부터 응답을 기다렸다가 응답이 오면 Future 객체(RecordMetadata 인스턴스)를 반환
        // 이 객체를 통해 ReocordMetadata의 비동기 결과를 표현, get() 메소드를 통해 데이터의 결과를 가져와서 레코드가 브로커에 정상 적재 되었는지 확인
        RecordMetadata metadata = producer.send(record).get();
        logger.info("{}", record);
        producer.flush();
        producer.close();
    }
}
