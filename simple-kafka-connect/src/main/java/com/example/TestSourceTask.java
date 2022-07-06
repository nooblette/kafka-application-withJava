package com.example;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;

public class TestSourceTask extends SourceTask {
    @Override
    public String version() { // 태스크의 버전을 지정, 커넥터의 version() 메소드와 동일한 버전으로 작성하는 것이 일반적
        return null;
    }

    @Override
    public void start(Map<String, String> props) { // 태스크가 시작할때 필요한 로직 작성
        // 데이터 처리에 필요한 모든 리소스를 이 메소드에서 초기화
        // 예를들어, JDBC 소스 커넥터를 구현한다면 이 메서드에서 JDBC 커넥션을 맺는것이 좋음

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException { // 소스 애플리케이션 또는 소스 파일로부터 데이터를 읽어오는 로직
        List<SourceRecord> sourceRecords; // 토픽으로 보낼 데이터를 SourceRecord로 정의
        return sourceRecords; // 리턴하면 데이터가 토픽으로 전송됨
    }

    @Override
    public void stop() { // 태스크가 종료될때 필요한 로직 작성
        // 예를들어, JDBC 소스 커넥터를 구현한다면 이 메서드에서 JDBC 커넥션을 종료

    }
}
