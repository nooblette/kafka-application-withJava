package com.example.SpringProducer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;릿
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration // KafkaTemplate 빈 객체를 등록하기 위해 Configuration 어노테이션을 선언
public class KafkaTemplateConfiguration {

    @Bean
    public KafkaTemplate<String, String> customKafkaTemplate() { // customerKafkaTemplate 이름으로 빈 객체가 생성

        Map<String, Object> props = new HashMap<>();
        // ProducerFactory를 사용하여 KafkaTemplate 객체를 만들때에는 프로듀서 옵션을 직접 설정
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(props); // KafkaTemplate 객체를 만들기 위한 ProducerFactory 초기화

        return new KafkaTemplate<>(pf); // 빈 객체로 사용할 KafkaTemplate 인스턴스를 초기화하고 리턴
        // ReplyingKafkaTemplate 이나 RoutingKafkaTemplate 인스턴스를 리턴하여 빈 객체로 등록하여, 다양항 옵션의 KafkaTemplate 를 사용 가능
    }
}
