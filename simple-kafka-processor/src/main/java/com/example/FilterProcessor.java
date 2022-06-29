package com.example;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Processor;

public class FilterProcessor implements Processor<String, String> { // 스트림 프로세서 클래스를 생성하기 위해서 Processor 인터페이스를 구현

    private ProcessorContext context; // 프로세서에 대한 정보(현재 스트림 처리 중인 토폴로지의 토픽, 애플리케이션 아이디 등..)를 조회

    @Override
    public void init(ProcessorContext context){ // 스트림 프로세서 생성자
        this.context = context;
    }

    @Override
    public void process(String key, String value) { // 프로세싱 로직이 들어가는 부분
        if(value.length() > 5){ // 메시지 값의 길이가 5이상인 경우에만
            context.forward(key, value); // 다음 토폴로지(다음 프로세서)로 넘기도록
        }
        context.commit(); // 처리가 완료된 이후 commit을 호출하여 명시적으로 데이터가 처리 되었음을 선언
    }

    @Override
    public void close() { // 프로세싱 하기 위해 사용했던 리소스를 해제
    }
}
