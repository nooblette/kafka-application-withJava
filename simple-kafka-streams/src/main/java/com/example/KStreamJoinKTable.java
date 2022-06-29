package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class KStreamJoinKTable {

    private static String APPLICATION_NAME = "streams-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String ADDRESS_TABLE = "address"; // 생성해둔 address 토픽을 사용하기 위해 String으로 선언
    private static String ORDER_STREAM = "order"; // 생성해둔 order 토픽을 사용하기 위해 String으로 선언
    private static String ORDER_JOIN_STREAM = "order_join"; // 생성해둔 order_join 토픽을 사용하기 위해 String으로 선언

    public static void main(String[] args){

        Properties props= new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder(); // Stream Topology 정의
        KTable<String, String> addressTable = builder.table(ADDRESS_TABLE);
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM); // order 토픽을 KStream 으로 가져올 것이므로 stream() 메서드를 소스 프로세서로 사용

        orderStream.join(addressTable, // KStream 의 stream() 메소드를 소스 프로세서로 addressTable과 join
                (order, address) -> order + " send to " + address) // "order 토픽의 물품 이름"과 "address 토픽의 주소"를 결합하여 새로운 message value 생성
                .to(ORDER_JOIN_STREAM); // to() 싱크 프로세서를 사용하여 order_join 토픽에 저장

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
