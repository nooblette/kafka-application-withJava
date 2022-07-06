package com.example;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class SingleFileSinkTask extends SinkTask {
    private SingleFileSinkConnectorConfig config;
    private File file;
    private FileWriter fileWriter;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) { // 커넥터를 실행할 때 설정한 옵션을 토대로 리소스를 초기화
       try {
            config = new SingleFileSinkConnectorConfig(props);

            // 파일에 데이터를 저장하므로 1개의 파일 인스턴스를 생성
            // 파일 위치와 이름은 SingleFileSinkConnectorConfig 에서 설정한 옵션값을 기준으로 설정
            file = new File(config.getString(config.DIR_FILE_NAME));
            fileWriter = new FileWriter(file, true);
        } catch (Exception e){
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) { // 일정 주기로 토픽의 데이터를 가져오는 메소드. 데이터를 저장하는 코드 작성
        try {
            for (SinkRecord record : records){ // SinkRecord : 토픽의 레코드
                fileWriter.write(record.value().toString() + "\n"); // 레코드의 메시지 값(value() 메서드로 리턴받음)을 String 포맷으로 저장
            }
        } catch (IOException e){
             throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    // put 메서드는 버퍼에 데이터를 저장하므로
    // FileWriter 클래스의 flush() 메서드를 호출해서 실질적으로 파일 시스템에 데이터를 저장
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets){
        try {
            fileWriter.flush();
        } catch (IOException e){
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public void stop() {
        try {
            fileWriter.close();
        } catch (IOException e){
            throw new ConnectException(e.getMessage(), e);
        }
    }
}
