package com.pipeline;

import com.pipeline.consumer.ConsumerWorker;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.jetty.util.component.Graceful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class HdfsSinkApplication {
    private final static Logger logger = LoggerFactory.getLogger(HdfsSinkApplication.class);

    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String TOPIC_NAME = "select-color";
    private final static String GROUP_IP = "color-hdfs-save-consumer-group";
    private final static int CONSUMER_COUNT = 3; // 생성할 스레드 개수를 변수로 선언, 추후 파티션이 늘어나더라도 변수를 변경하고 재배포,실행하여 파티션과 동일 개수의 컨슈머 스레드를 동적으로 운영 가능
    private final static List<ConsumerWorker> workers = new ArrayList<>();

    public static void main(String[] args){
        Runtime.getRuntime().addShutdownHook(new ShutdownThread()); // 안전한 컨슈머의 종료를 위해 Shutdown Hook 을 선언

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_IP);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 컨슈머에 필요한 설정을 미리 정의해서 각 컨슈머 스레드에 Properties 인스턴스(configs)를 넘김
        ExecutorService executorService = Executors.newCachedThreadPool(); // 컨슈머 스레드를 스레드 풀로 관리하기 위한 newCachedThreadPool() 메서드
        for(int i = 0; i < CONSUMER_COUNT; i++){
            workers.add(new ConsumerWorker(configs, TOPIC_NAME, i)); // 컨슈머 스레드를 CONSUMER_COUNT 개수만큼 생성, 생성된 컨슈머 스레드 인스턴스들을 묶음으로 관리하기 위해 List로 추가
        }
        workers.forEach(executorService::execute); // 컨슈머 스레드 인스턴스들을 스레드 풀에 포함시 실행

    }

    // 컨슈머의 안전한 종료를 위한 Shutdown Hook 로직
    static class ShutdownThread extends Thread {
        public void run() {
            logger.info("Shutdown hook");
            workers.forEach(ConsumerWorker::stopAndWakeup); // Shutdown Hook 이 발생했을 경우 각 컨슈머 스레드 종료를 알리도록 명시적으로 stopAndWakeup() 메서드를 호출
        }
    }
}
