package com.pipeline;

public class UserEventVO {
    public UserEventVO(String timestamp, String userAgent, String colorName, String userName){
        this.timestamp = timestamp;
        this.userAgent = userAgent;
        this.colorName = colorName;
        this.userName = userName;
    }
    private String timestamp; // REST API 호출 시점을 메시지 값에 넣는 역할, 컨슈머에서 병렬처리로 인해 적재 시점이 달라지더라도 최종 적재된 값을 이 값을 기준으로 정렬하여 호출 시간 순서대로 처리 가능
    private String userAgent; // REST API 호출 받을때 받을 수 있는 브라우저의 종류
    private String colorName; // 사용자의 입력값
    private String userName; // 사용자의 입력값
}
