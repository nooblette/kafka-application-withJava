package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.rmi.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SingleFileSinkConnector extends SinkConnector {

    private Map<String, String> configProperties;

    @Override
    public void start(Map<String, String> props) { // SingleFileSinkConnector 커넥터를 생성할 때 받은 설정값들을 초기화
        this.configProperties = props;
        try {
            new SingleFileSinkConnectorConfig(props); // 커넥트에서 SingleFileSourceConnector 커넥터를 생성할때 받은 설정값들을 초기화
        } catch (ConfigException e){
            throw new ConfigException(e.getMessage(), e); // 필수 설정값이 빠져있다면 ConfigException 에러 반환
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SingleFileSinkTask.class; // SingleFileSinkConnector 가 사용할 태스크의 클래스 이름을 지정
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) { // 태스크가 2개 이상이고 태스크마다 다른 설정값을 줄 때 사용
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties); // 여기선 동일한 설정값들을 받도록 ArrayList<>에 모두 동일한 설정을 담음
        for(int i = 0; i < maxTasks; i++){
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() { // 커넥터에서 사용할 설정값을 지정
        return SingleFileSinkConnectorConfig.CONFIG; // SingleFileSinkConnectorConfig의 CONFIG 인스턴스를 리턴
    }

    @Override
    public String version() {
        return "1.0";
    }
}
