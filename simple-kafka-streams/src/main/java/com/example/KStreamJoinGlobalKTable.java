package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class KStreamJoinGlobalKTable {

    private static String APPLICATION_NAME = "streams-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String ADDRESS_TABLE = "address_v2"; // 생성해둔 address_v2 토픽을 사용하기 위해 String으로 선언
    private static String ORDER_STREAM = "order"; // 생성해둔 order 토픽을 사용하기 위해 String으로 선언
    private static String ORDER_JOIN_STREAM = "order_join"; // 생성해둔 order_join 토픽을 사용하기 위해 String으로 선언

    public static void main(String[] args){

        Properties props= new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder(); // Stream Topology 정의

        // address_v2를 GlobalKTable로 정의하기 위해 globalTable() 메소드 호출
        // globalTable() 메소드는 토픽을 GlobalKTable 인스턴스로 만드는 소스 프로세서 역할
        GlobalKTable<String, String> addressGlobalTable = builder.globalTable(ADDRESS_TABLE);

        // order 토픽은 KStream으로 사용하기 위해 stream() 메소드를 소스 프로세서로 호출
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM);

        orderStream.join(addressGlobalTable, // KStreamJoinKTable 에서 사용했던 join 메소드를 오버로딩하여 사용
                        (orderKey, orderValue) -> orderKey, // KStream(order topic)의 메시지 키를 GlobalKTable(address_v2 topic)의 메시지 키와 매칭하여 JOIN
                        (order, address) -> order + " send to " + address) // 새로운 데이터 새애성
                        .to(ORDER_JOIN_STREAM); // to() 메소드를 싱크 프로세서로 order_join 토픽에 저장

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
