package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 클래스명(SingleFileSourceConnector)가 커넥트에서 사용할 커넥터 이름이 됨
// 플러그인으로 추가하여 사용시 패키지음과 함께 붙여서 사용해야함
public class SingleFileSourceConnector extends SourceConnector {

    private Map<String, String> configProperties;

    @Override
    public void start(Map<String, String> props) {
        this.configProperties = props;
        try {
            new SingleFileSourceConnectorConfig(props); // 커넥트에서 SingleFileSourceConnector 커넥터를 생성할때 받은 설정값들을 초기화
        } catch (ConfigException e){
            throw new ConfigException(e.getMessage(), e); // 필수 설정값이 빠져있다면 ConfigException 에러 반환
        }
    }

    @Override
    public Class<? extends Task> taskClass() { // SingleFileSourceConnector 커넥터가 사용할 태스크의 이름을 지정
        return SingleFileSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) { // 태스크가 2개 이상인 경우 태스크마다 다른 설정값을 줄 때 사용
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties);
        for(int i = 0; i < maxTasks; i++){
            taskConfigs.add(taskProps); // 여기서는 태스크가 2개 이상이더라도 동일한 설정값을 받도록 ArrayList()에 동일한 설정을 담음
        }
        return taskConfigs;
    }

    @Override
    public void stop() { // 커넥터가 종료될때 필요한 로직 추가
        // 커넥터 종료시 해제해야할 리소스가 있으면 이 메소드에 명시
    }

    @Override
    public ConfigDef config() { // 커넥터에서 사용할 설정값을 지정, SingleFileSourceConnectorConfig에 정의된 CONFIG 인스턴스를 리턴
        return SingleFileSourceConnectorConfig.CONFIG;
    }

    @Override
    public String version() {
        return "1.0";
    }
}
