package com.pipeline;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
@CrossOrigin(origins = "*", allowedHeaders = "*") // RestController 를 다른 도메인에서도 호출할 수 있도록 CORS를 설정하기 위함
public class ProducerController {
    private final Logger logger = LoggerFactory.getLogger(ProducerController.class);

    private final KafkaTemplate<String, String> kafkaTemplate; // 카프카 스트링의 kafkaTemplate 인스턴스를 생성

    public ProducerController(KafkaTemplate<String, String> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/api/select")
    public void selectColor(
            @RequestHeader("user-agent") String userAgentName,
            @RequestParam(value = "color") String colorName,
            @RequestParam(value = "user") String userName) {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ"); // 최종 적재되는 시간을 저장
        Date now = new Date();
        Gson gson = new Gson();
        UserEventVO userEventVO = new UserEventVO(
                sdfDate.format(now),
                userAgentName,
                colorName,
                userName);
        String jsonColorLog = gson.toJson(userEventVO); // 자바 객체(UserEventVO)를 JSON 포맷의 String 타입으로 전환
        kafkaTemplate.send("select-color", jsonColorLog).addCallback( // select-color 토픽에 데이터를 전송, 데이터가 정상적으로 전송되었는지 확인하기 위해 addCallback() 메소드를 추가
                new ListenableFutureCallback<SendResult<String, String>>() {
                    @Override
                    public void onSuccess(SendResult<String, String> result) { // 전송 성공 내역을 로그로 남김
                        logger.info(result.toString());
                    }

                    @Override
                    public void onFailure(Throwable ex){ // 전송 실패 내역을 로그로 남김
                        logger.error(ex.getMessage(), ex);
                    }
                }
        );
    }
}
