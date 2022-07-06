package com.example;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SingleFileSourceTask extends SourceTask {
    private Logger logger = LoggerFactory.getLogger(SingleFileSourceTask.class);

    // 아래 두 값을 기준으로 오프셋 스토리지에 읽은 위치를 저장
    public final String FILENAME_FIELD = "filename"; // 데이터를 읽을 파일 이름 지정
    public final String POSITION_FIELD = "position"; // 해당 파일을 읽은 지점을 저장

    // offset storage 에 데이터를 저장하고 읽기 위해 사용(Map 자료구조에 담은 데이터를 사용)
    private Map<String, String> fileNamePartition; // filename = key, value = 커넥터가 읽는 파일 이름 으로 지정
    private Map<String, Object> offset;
    private String topic; // 토픽 이름
    private String file; // 파일 이름

    private long position = -1;// 읽은 파일의 위치를 커넥터 멤버변수로 지정하여 사용(처음 읽는 파일이라면 읽은 기록이 없으므로 0으로 설정하여 사용)

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        try{
            //Init variables

            // 커넥터 실행시 받은 설정값을 SingleFileSourceConnectorConfig로 선언하여 사용
            SingleFileSourceConnectorConfig config = new SingleFileSourceConnectorConfig(props);

            // 다른 메소드에서 사용할 수 있도록 토픽 이름과 파일 이름을 초기화
            topic = config.getString(SingleFileSourceConnectorConfig.TOPIC_NAME);
            file = config.getString(SingleFileSourceConnectorConfig.DIR_FILE_NAME);

            // 오피셋 스토리지(offset storage) - 실제로 데이터가 저장되는 곳
            // 오프셋 스토리지에서 현재 읽고자 하는 파일 정보를 가져옴
            fileNamePartition = Collections.singletonMap(FILENAME_FIELD, file);
            offset = context.offsetStorageReader().offset(fileNamePartition); // null이라면 읽고자하는 데이터가 없다는 뜻

            // Get file offset from offsetStorageReader
            if (offset != null) {
                Object lastReadFileOffset = offset.get(POSITION_FIELD); // 마지막으로 파일의 읽은 위치를 get() 메소드로 가져옴
                if(lastReadFileOffset != null){
                    position = (Long) lastReadFileOffset; // 마지막으로 데이터를 가져오고 처리한 지점을 position 변수에 할당
                }
            } else {
                position = 0; // 오프셋 스토리지부터 가져온 데이터가 null이라면 파일을 처리한 적이 없다는 뜻이므로 0을 할당
            }
        } catch (Exception e) {
            throw new ConfigException(e.getMessage(), e);
        }
    }

    @Override
    // 소스 파일로부터 데이터를 읽어서 토픽으로 전송하는 메소드
    // 태스크가 시작한 이후 지속적으로 데이터를 가져오기 위해 반복적으로 호출
    public List<SourceRecord> poll() throws InterruptedException {

        List<SourceRecord> results = new ArrayList<>(); // List<SourceRecord>를 리턴하여 토픽으로 데이터를 전송
        try {
            Thread.sleep(1000);

            List<String> lines = getLines(position); // 마지막으로 읽었던 지점 이후로 파일의 마지막 지점까지 읽어서 리턴하는 getLines() 메서드로 데이터를 받음

            if (lines.size() > 0) { // 토픽으로 보내기 전에 파일에서 한 줄씩 데이터를 읽어옴
                lines.forEach(line -> {
                    Map<String, Long> sourceOffset = Collections.singletonMap(POSITION_FIELD, ++position);

                    /*
                     fileNamePartition 과 sourceOffset(현재 토픽으로 보내눈 줄의 위치를 기록)을 파라미터로 넣고
                     각 줄을 한개의 SourceRecord 인스턴스로 생성하여 result 변수에 추가
                     */
                    // 마지막으로 전송한 데이터의 위치를 오프셋 스토리지에 기록
                    SourceRecord sourceRecord = new SourceRecord(fileNamePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line);
                    results.add(sourceRecord);
                });
            }
            return results;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new ConnectException(e.getMessage(), e);
        }
    }

    private List<String> getLines(long readLine) throws Exception{
        BufferedReader reader = Files.newBufferedReader(Paths.get(file));
        return reader.lines().skip(readLine).collect(Collectors.toList());
    }

    @Override
    public void stop() {

    }
}
