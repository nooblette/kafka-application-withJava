package com.example;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class TestSinkTask extends SinkTask {
    @Override
    public String version() { // 태스크의 버전을 지정, 커넥터의 version() 메서드에서 지정한 버전과 동일하게 작성하는 것이 일반적
        return null;
    }

    @Override
    public void start(Map<String, String> props) { // 태스크가 시작할 때 필요한 로직을 작성
        // 태스크가 실질적으로 데이터를 처리하므로 데이터 처리에 필요한 리소스를 여기서 초기화
        // MongoDB 싱크 커넥터라면 여기서 mongoDB 커넥터를 맺음
    }

    @Override
    public void put(Collection<SinkRecord> records) { // 싱크 애플리케이션에 저장할 데이터를 토픽에서 주기적으로 가져오는 메서드
        //SinkRecord 는 토픽의 한 개 레코드이며 토픽, 파티션, 타임스탬프 등의 정보를 담음
        // 여러개의 SinkRecord 를 묶어서 파라미터로 사용할 수 있음

    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) { // 옵션(경우에 따라 사용)
        // put() 메서드를 통해 가져온 데이터를 일정 주기로 싱크 애플리케이션 혹은 싱크 파일에 저장
        // e.g) put() 메소드로 데이터를 insert 하고 flush() 메소드로 commit 을 수행하여 트랜잭션을 끝낼 수 있음
    }

    @Override
    public void stop() { // 태스크가 종료될 때 필요한 로직을 작성
        // 태스크에서 사용한 리소스를 해제해야 한다면 여기에 종료 코드를 구현

    }
}
