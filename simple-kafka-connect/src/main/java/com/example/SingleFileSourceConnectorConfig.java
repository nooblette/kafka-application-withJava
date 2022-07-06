package com.example;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class SingleFileSourceConnectorConfig extends AbstractConfig {

    // 어떤 파일을 읽을 것인지 파일의 위치와 이름에 대한 정보를 지정
    public static final String DIR_FILE_NAME = "file";
    public static final String DIR_FILE_NAME_DEFAULT_VALUE = "/tmp/kafka.txt";
    private static final String DIR_FILE_NAME_DOC = "읽을 파일의 경로와 이름";

    // 읽은 파일을 어느 토픽으로 보낼 것인지 지정
    public static final String TOPIC_NAME = "topic";
    private static final String TOPIC_DEFAULT_VALUE = "test";
    private static final String TOPIC_DOC = "보낼 토픽 이름";

    // ConfigDef : 커넥터에서 사용할 옵션 값들에 대한 정의를 표현, 각 옵션값의 이름/설명/기본값/중요도를 지정
    public static ConfigDef CONFIG = new ConfigDef().define(DIR_FILE_NAME,
                    ConfigDef.Type.STRING,
                    DIR_FILE_NAME_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    DIR_FILE_NAME_DOC)
            .define(TOPIC_NAME,
                    ConfigDef.Type.STRING,
                    TOPIC_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    TOPIC_DOC);

    public SingleFileSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }
}
