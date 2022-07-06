package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.List;
import java.util.Map;

// SinkConnector 클래스를 상속받아 구현

// 상속받은 사용자 정의 클래스르 선언
// 클래스 이름이 최종적으로 커넥트에서 호출 될 때 사용
// 어떻게 사용되는 커넥터인지 명확하게 네이밍 (e.g.) MongoDbSinkConnector)
public class TestSinkConnector extends SinkConnector {
    @Override
    public void start(Map<String, String> props) { // 사용자가 JSON 또는 config 파일 형태로 입력한 설정값을 초기화
        // 올바른 값이 아니라면 ConnectException()을 호출하여 커넥터를 종료할 수 있음

    }

    @Override
    public Class<? extends Task> taskClass() { // 이 커넥터가 사용할 태스크 클래스를 지정
        return null;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) { // 태스크가 2개 이상이고 각 태스크마다 각기 다른 옵션을 지정할때 사용
        return null;
    }

    @Override
    public void stop() { // 커넥터가 종료될 때 필요한 로직을 작성

    }

    @Override
    public ConfigDef config() { // 커넥터에서 사용할 설정값을 지정, 커넥터 설정값은 ConfigDef 클래스를 통해 각 설정의 이름, 기본값, 중요도, 설명을 정의 가능
        return null;
    }

    @Override
    public String version() { // 커넥트의 버전을 리턴
        return null;
    }
}
