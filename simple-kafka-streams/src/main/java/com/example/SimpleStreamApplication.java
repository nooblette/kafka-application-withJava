package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class SimpleStreamApplication {

    private static String APPLICATION_NAME = "streams-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_COPY = "stream_log_copy";

    public static void main(String[] args){

        Properties props= new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME); // 애플리케이션 아이디 값을 기준으로 병렬처리
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); // 스트림즈 애플리케이션과 연동할 카프카 클러스터 정보
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // 메시지 key 직렬화, 역직렬화 방식
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // 메시지 value 직렬화, 역직렬화 방식

        StreamsBuilder builder = new StreamsBuilder(); // Stream Topology 정의
        KStream<String, String> streamLog = builder.stream(STREAM_LOG); // stream 메소드를 통해 'stream_log' 토픽으로부터 KStream 객체 생성
        streamLog.to(STREAM_LOG_COPY); // to() 메소드를 통해 stream_log 토픽을 담은 KStream 객체를 다른 토픽으로 전송

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
