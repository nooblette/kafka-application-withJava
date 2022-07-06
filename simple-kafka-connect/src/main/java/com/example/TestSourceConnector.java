package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

// 클래스 이름이 최종적으로 커넥트에서 호출 될 때 사용
// 어떻게 사용되는 커넥터인지 명확하게 네이밍 (e.g.) MongoDbSourceConnector)
public class TestSourceConnector extends SourceConnector {
    @Override
    public void start(Map<String, String> props) { // 사용자가 JSON 또는 config 파일 형태로 입력한 설정 값을 초기화
        // 올바른 값이 아니라면(커넥션 URL이 올바르지 않다면) ConnectException()을 호출하여 커넥터를 종료

    }

    @Override
    public Class<? extends Task> taskClass() { // 해당 커넥터가 사용할 태스크 클래스 지정
        return null;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) { // 태스크가 2개 이상이고 각 태스크마다 각기 다른 옵션을 지정할때 사용
        return null;
    }

    @Override
    public void stop() { // 커넥터가 종료될때 필요한 로직 작성

    }

    @Override
    public ConfigDef config() { // 커넥터가 사용할 설정값에 대한 정보를 받음(설정의 이름, 기본값, 중요도, 설명 등..)
        return null;
    }

    @Override
    public String version() { // 커넥터의 버전을 리턴, 커넥터를 유지보수하고 신규 배포할때 이 메서드가 리턴하는 버전 값을 변경해야함
        return null;
    }
}
