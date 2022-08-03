package com.pipeline.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class ElasticSearchSinkConnectorConfig extends AbstractConfig {
    public static final String ES_CLUSTER_HOST = "es.host"; // 토픽의 데이터를 저장할 엘라스틱서치 호스트 이름을 설정으로 선언
    private static final String ES_CLUSTER_HOST_DEFAULT_VALUE = "localhost";
    private static final String ES_CLUSTER_HOST_DOC = "엘라스틱서치 호스트를 입력";

    public static final String ES_CLUSTER_PORT= "es.port"; // 토픽의 데이터를 저장할 엘라스틱서치 포트 이름을 설정으로 선언
    private static final String ES_CLUSTER_PORT_DEFAULT_VALUE = "9200";
    private static final String ES_CLUSTER_PORT_DOC = "엘라스틱서치 포트를 입력";

    public static final String ES_INDEX = "es.index"; // 토픽의 데이터를 저장할 때 설정할 데이터의 인덱스 이름을 설정으로 선언
    private static final String ES_INDEX_DEFAULT_VALUE = "kafka-connector-index";
    private static final String ES_INDEX_DOC = "엘라스틱서치 인덱스를 입력";

    public static ConfigDef CONFIG = new ConfigDef().define( // ConfigDef 인스턴스를 통해 커넥터에서 설정값이 정상적으로 들어왔는지 검증
            ES_CLUSTER_HOST, ConfigDef.Type.STRING,
            ES_CLUSTER_HOST_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
            ES_CLUSTER_HOST_DOC)
            .define(ES_CLUSTER_PORT, ConfigDef.Type.INT,
                    ES_CLUSTER_PORT_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    ES_CLUSTER_PORT_DOC)
            .define(ES_INDEX, ConfigDef.Type.STRING,
                    ES_INDEX_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    ES_INDEX_DOC);

    public ElasticSearchSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }
}
