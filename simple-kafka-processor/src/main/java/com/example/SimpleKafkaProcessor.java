package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class SimpleKafkaProcessor {

    private static String APPLICATION_NAME = "processor-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new Topology(); // 프로세서 API를 사용한 토폴로지를 구성
        topology.addSource("Source", STREAM_LOG) // stream_log 토픽을 소스 프로세서 가져오기 위함, addSource(소스 프로세서의 이름, 대상 토픽의 이름)
                .addProcessor("Process", // 스트림 프로세서를 사용하기 위함, addProcessor(스트림 프로세서의 이름, 사용자가 정의한 프로세서 인스턴스, 토폴로지의 부모노드)
                        () -> new FilterProcessor(),
                        "Source") // 부모노드는 "Source" 즉, Source processor -> Process processor 로 스트리밍
                .addSink("Sink", // 싱크 프로세서를 사용하여 데이터를 저장하기 위함, addSink(싱크 프로세서의 이름, 저장할 토픽의 이름, 토폴로지의 부모노드)
                        STREAM_LOG_FILTER,
                        "Process");  // 필터링 처리가 완료된 데이터를 가져와서 저장해야하므로 부모노드는 "Process"

        KafkaStreams streaming = new KafkaStreams(topology, props);
        streaming.start();
    }
}
