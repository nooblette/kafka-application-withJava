package com.pipeline.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerWorker implements Runnable{ // HdfsSinkApplication 에서 전달받은 Properties 인스턴스로 컨슈머를 생성 및 실행하는 Thread Class
    private final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);

    // 컨슈머의 poll 메서드를 통해 전달받은 데이터를 임시 저장하는 버퍼(flush 방식으로 저장하기 때문에 버퍼가 필요)
    // static 변수로 선언하여 다수의 스레드가 만들어지더라도 동일 변수에 접근
    // 멀티 스레드 환경에서 안전하게 사용할 수 있는 ConcurrentHashMap<> 으로 구현
    private static Map<Integer, List<String>> bufferString = new ConcurrentHashMap<>(); // <파티션번호, 메시지값 들> 이 들어감

    private static Map<Integer, Long> currentFileOffset = new ConcurrentHashMap<>(); // 오프셋 값을 저장하고 파일 이름을 저장할 때 오프셋 번호를 붙이는데 사용

    private final static int FLUSH_RECORD_COUNT = 10;
    private Properties prop;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;


    public ConsumerWorker(Properties prop, String topic, int number){
        logger.info("Generate ConsumerWorker");
        this.prop = prop;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number; // 스레드에 이름과번호를 붙여서 로깅시 편리하게 스레드 번호를 확인
    }
    @Override
    public void run() {
        Thread.currentThread().setName(threadName);
        consumer = new KafkaConsumer<>(prop); // 스레드를 생성하는 HdfsSinkApplication에서 설정한 컨슈머 설정을 가져와서 KafkaConsumer 인스턴스를 생성하고
        consumer.subscribe(Arrays.asList(topic)); // 해당되는 토픽을 구독
        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for(ConsumerRecord<String, String> record : records){
                    addHdfsFileBuffer(record); // poll 메서드로 가져온 데이터를 버퍼에 쌓음
                }
                // 버퍼에 쌓인 데이터가 일정 개수를 넘었을 경우 HDFS에 저장(flush 방식)
                // assignment() 메소드를 통해서 현재 컨슈머 스레드에 할당된 파티션에 대한 버퍼 데이터만 적재
                saveBufferToHdfsFile(consumer.assignment());
            }
        }catch (WakeupException e){
            logger.warn("Wakeup consumer"); // 안전한 종료를 위해 WakeUpException 을 받아서 컨슈머 종료 과정을 수행
        } catch (Exception e){
            logger.error(e.getMessage(), e);
        } finally {
            consumer.close();
        }
    }

    /* HDFS 적재를 위한 로직 */
    private void addHdfsFileBuffer(ConsumerRecord<String, String> record){ // 레코드를 받아서 메시지 값을 버퍼에 넣는 코드
        List<String> buffer = bufferString.getOrDefault(record.partition(), new ArrayList<>());
        buffer.add(record.value());
        bufferString.put(record.partition(), buffer);

        if (buffer.size() == 1) // 만일 버퍼크기가 1이라면 버퍼의 가장 처음 오프셋 이라는 의미이므로 currnetFileOffset 변수에 넣는다
            /*
             *currentFileOffset 변수에서 오프셋 번호를 관리하면 파일을 저장할 때 파티션 이름과 오프셋 번호를 붙여서 저장할 수 있어
             * 이슈 발생시 파티션과 오프셋에 대한 정보를 알 수 있음
             */
            currentFileOffset.put(record.partition(), record.offset());
    }

    private void saveBufferToHdfsFile(Set<TopicPartition> partitions){ // 컨슈머로부터 Set<TopicPartition> 정보를 받아서 컨슈머 스레드에 할당된 파티션에만 접근(데드락 방지)
        partitions.forEach(p -> checkFlushCount(p.partition())); // 버퍼의 데이터가 flush 될 만큼 충족되었는지 확인하기 위해 checkFlushCount 메서드를 호출
    }

    private void checkFlushCount(int partitionNo){ // 파라미터로 받은 파티션 변수의 버퍼를 확인하여 flush 를 수행할 만큼 레코드가 찼는지 확인
        if (bufferString.get(partitionNo) != null){
            if(bufferString.get(partitionNo).size() > FLUSH_RECORD_COUNT - 1){ // 일정 개수 이상만큼 채워져 있으면 HDFS 적재로직인 save() 메서드를 호출
                save(partitionNo);
            }
        }
    }

    private void save(int partitionNo){ // 실질적인 HDFS 적재를 수행
        if(bufferString.get(partitionNo).size() > 0)
            try{
                String fileName = "/data/color-" + partitionNo + "-" + currentFileOffset.get(partitionNo) + ".log"; // HDFS 에 저장할 파일 이름을 설정(파일 이름만 보고도 토픽 정보를 알 수 있도록 분명히 명시)
                Configuration configuration = new Configuration(); // HDFS 적재를 위한 설정을 수행
                configuration.set("fs.defaultFS", "hdfs://localhost:9000"); // hdfs://localhost:9000를 대상으로 FileSystem 인스턴스를 생성
                FileSystem hdfsFileSystem = FileSystem.get(configuration);

                FSDataOutputStream fileOutputStream = hdfsFileSystem.create(new Path(fileName)); // 버퍼 데이터를 파일로 저장하기 위해 FSDataOutputStream 인스턴스를 생성
                fileOutputStream.writeBytes(StringUtils.join(bufferString.get(partitionNo), "\n")); // 버퍼에 쌓인 데이터를 fileOutPutStream 에 저장
                fileOutputStream.close(); // 저장이 완료된 후에는 close() 메서드로 안전하게 종료

                bufferString.put(partitionNo, new ArrayList<>()); // 버퍼 데이터가 적재 완료되었다면 버퍼 데이터를 초기화
            } catch (IOException e) { // 에러 발생시 로그를 남김
                e.printStackTrace();
            }

    }

    /* 안전한 컨슈머 스레드 종료를 위한 로직 */
    public void stopAndWakeup() { // Shutdown Hook 이 발생했을 때 안전한 정료를 위해
        logger.info("stopAndWakeup");
        consumer.wakeup(); // consumer 의 wakeup() 메서드를 호출
        saveRemainBufferToHdfsFile(); // 만일 버퍼에 데이터가 남아있다면 남은 버퍼의 데이터를 모두 저장하기 위한 메서드 호출
    }

    private void saveRemainBufferToHdfsFile() { // 버퍼에 남아있는 모든 데이터를 HDFS 에 저장하기 위한 메소드, 컨슈머 스레드 종료시에 호출
        bufferString.forEach((partitionNo, v) -> this.save(partitionNo));
    }
}
