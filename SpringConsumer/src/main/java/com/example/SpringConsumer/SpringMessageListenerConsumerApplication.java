package com.example.SpringConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

@SpringBootApplication
public class SpringMessageListenerConsumerApplication {
	public static Logger logger = LoggerFactory.getLogger(SpringMessageListenerConsumerApplication.class);

	public static void main(String[] args) {

		SpringApplication springApplication = new SpringApplication(SpringMessageListenerConsumerApplication.class);
		springApplication.run(args);
	}

	/* 가장 기본적인 리스너 선언 */
	@KafkaListener(topics = "test",
			groupId = "test-group-00") // KafkaListener 어노테이션으로 topics 과 group id를 지정
	public void recordListener(ConsumerRecord<String, String> record){ // poll() 메서드가 호출되어 가져온 레코드들의 각 메시지 값을 파라미터로 전달받음
		logger.info(record.toString()); // 메시지 키, 메시지 값에 대한 처리를 이 안에서 수행
	}


	/* 메시지 값을 파라미터로 받는 리스너 */
	@KafkaListener(topics = "test",
			groupId = "test-group-01")
	public void singleTopicListener(String messageValue){ // 역직렬화 클래스로 StringDeserializer 를 사용했으므로 String 클래스를 메시지 값으로 전달 받음
		logger.info(messageValue);
	}


	/* properties : 개별 리스너에 카프카 컨슈머 옵션값을 부여 */
	@KafkaListener(topics = "test",
			groupId = "test-group-02",
			properties = {
							"max.poll.interval.ms:60000",
							"auto.offset.reset:earliest"
			})
	public void singleTopicWithPropertiesListener(String messageValue){
		logger.info(messageValue);
	}


	/* concurrency : 2개 이상의 카프카 컨슈머 스레드를 실행 */
	@KafkaListener(topics = "test",
			groupId = "test-group-03",
			concurrency = "3") // 3개의 컨슈머 스레드를 만들어서 병렬처리
	public void concurrentTopicListener(String messageValue){
		logger.info(messageValue);
	}


	/* topicPartitions(Parameter) : 특정 토픽의 특정 파티션만 구독
	 * PartitionOffset(Annotation) : 특정 파티션의 특정 오프셋까지 지정
	 */
	@KafkaListener(topicPartitions = {
			@TopicPartition(topic = "test01", partitions = {"0", "1"}),
			@TopicPartition(topic = "test02", partitionOffsets =
			@PartitionOffset(partition = "0", initialOffset = "3"))
			},
			groupId = "test-group-04")
	public void listenSpecificPartition(ConsumerRecord<String, String> record){
		logger.info(record.toString());
	}

}
