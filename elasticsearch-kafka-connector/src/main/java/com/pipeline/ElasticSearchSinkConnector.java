package com.pipeline;

import com.pipeline.config.ElasticSearchSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 커넥터를 생성했을 때 최초로 실행
// 직접적으로 데이터를 적재하는 로직을 포함하는 것은 아니고, 태스크를 실행할 전 단계로써 설정값을 확인하고 태스크 클래스를 지정하는 역할
public class ElasticSearchSinkConnector extends SinkConnector {

    private final Logger logger = LoggerFactory.getLogger(ElasticSearchSinkConnector.class);

    private Map<String, String> configProperties;

    @Override
    public void start(Map<String, String> props) { // 커넥터가 최초로 실행될 때 실행되는 구문
        this.configProperties = props;
        try {
            new ElasticSearchSinkConnectorConfig(props); // 설정값을 가져와서 ElasticSearchSinkConnectorConfig 인스턴스를 생성
        } catch (ConfigException e){
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() { // 태스크 클래스 역할을 할 클래스를 선언
        // 다수의 태스크를 운영할 경우 태스크 클래스 분기 로직을 넣어 사용자가 원하는 태스크 클래스를 선택할 수 있음
        return ElasticSearchSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties);
        for(int i = 0; i < maxTasks; i++){
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() { // 커넥터가 종료될 때 로그를 남김
        logger.info("Stop elasticSearch connector");
    }

    @Override
    public ConfigDef config() { // ElasticSearchSinkConnectorConfig 에서 설정한 설정값(CONFIG)를 리턴
        return ElasticSearchSinkConnectorConfig.CONFIG;
    }

    @Override
    public String version() { // 커넥터의 버전을  설정, 커넥터의 버전에 따른 변경사항을 확인하기 위해 versioning 을 할 때 필요
        return "1.0";
    }
}
