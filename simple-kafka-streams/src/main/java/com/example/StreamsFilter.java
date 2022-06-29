package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilter {

    private static String APPLICATION_NAME = "streams-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_FILTERED = "stream_log_filtered";

    public static void main(String[] args){

        Properties props= new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);
        KStream<String, String> filteredStream = streamLog.filter((key, value) -> value.length() > 5); // 메시지 value의 길이가 5보다 클때만 stream_log_filter 토픽으로 전송하도록 필터링
        filteredStream.to(STREAM_LOG_FILTERED);  // to() 메소드를 통해 filter()를 거친 stream_log 토픽을 담은 KStream 객체를 다른 토픽으로 전송

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
