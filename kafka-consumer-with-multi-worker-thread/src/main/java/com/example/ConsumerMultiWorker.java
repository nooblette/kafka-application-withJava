package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// Runnable 인터페이스를 구현하여 작성, 이 클래스는 스레드로 실행
public class ConsumerMultiWorker implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerMultiWorker.class);
    private String recordValue;

    ConsumerMultiWorker(String recordValue){
        this.recordValue = recordValue;
    }

    @Override
    public void run() { // run() 메소드 : 데이터를 처리할 구문을 작성
        logger.info("thread:{}\trecord:{}", Thread.currentThread().getName(), recordValue); // thread 이름과 record 클래스를 로그로 출력
    }
}
