package com.example.SpringConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class SpringCustomConsumerApplication {
    public static Logger logger = LoggerFactory.getLogger(SpringCustomConsumerApplication.class);

    public static void main(String[] args){
        SpringApplication application = new SpringApplication(SpringCustomConsumerApplication.class);
        application.run(args);
    }

    @KafkaListener(topics = "test,",
        groupId = "test-group",
        containerFactory = "customContainerFactory") // containerFactory 옵션을 customConatinerFactory(ListenerContainerConfiguration 클래스의 Bean 객체명)
    public void customListener(String data){
        logger.info(data);
    }
}
