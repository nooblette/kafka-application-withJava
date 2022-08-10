package com.pipeline;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class MetricStreams {

    private static KafkaStreams streams;

    public static void main(final String[] args){

        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        Properties props = new Properties(); // 스트림즈를 실행하기 위한 옵션들을 정의
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "metric-streams-application"); // 애플리케이션 이름을 지정
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // 메시지 키의 직렬화, 역직렬화 타입 정의
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // 메시지 값의 직렬화, 역직렬화 타입 정의

        StreamsBuilder builder = new StreamsBuilder(); // 카프카 토폴로지를 정의
        KStream<String, String> metrics = builder.stream("metric.all"); // metric.all : 최초로 가져올 토픽, 이를 기반으로 KStream 인스턴스 생성
        KStream<String, String>[] metricBranch = metrics.branch( // branch() 메서드를 사용하여 메시지 값을 기준으로 KSTream을 양갈래로 분기처리(cpu or memory)
                (key, value) -> MetricJsonUtils.getMetricName(value).equals("cpu"),
                (key, value) -> MetricJsonUtils.getMetricName(value).equals("memory")
                );


        metricBranch[0].to("metric.cpu");
        metricBranch[1].to("metric.memory");

        KStream<String, String> filteredCpuMetric = metricBranch[0].filter((
                (key, value) -> MetricJsonUtils.getTotalCpuPercent(value) > 0.5 // cpu 사용량이 50%를 넘어가는 경우를 필터링
                ));

        // cpu 사용량이 50%를 넘어갈경우 데이터의 메시지 값을 모두 전송하는게 아니라
        // hostname과 tiemstamp 2개의 값을 조합하는 getHostTimeStamp() 메서드를 호출하여 변환된 형태로 전송
        // 변환된 데이터는 to() 메서드에 정의된 metric.cpu.alert 토픽으로 전달
        filteredCpuMetric.mapValues(value -> MetricJsonUtils.getHostTimeStamp(value)).to("metric.cpu.alert");

        streams = new KafkaStreams(builder.build(), props); // 정의된 토폴로지와 스트림즈 설정값으로
        streams.start(); // 카프카 스트림즈 실행
    }

    static class ShutdownThread extends Thread { // 스트림즈의 안전한 종료를 위해 셧다운 훅을 받을경우
        public void run(){
            streams.close(); // cloe() 메서드를 호출하여 안전하게 스트림즈를 종료
        }
    }
}
