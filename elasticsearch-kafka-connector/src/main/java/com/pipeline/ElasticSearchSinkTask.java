package com.pipeline;

import com.google.gson.Gson;
import com.pipeline.config.ElasticSearchSinkConnectorConfig;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class ElasticSearchSinkTask extends SinkTask { // 실질적인 ElasticSearch 처리 로직이 들어갈 클래스

    private final Logger logger = LoggerFactory.getLogger(ElasticSearchSinkTask.class);

    private ElasticSearchSinkConnectorConfig config;
    private RestHighLevelClient esClient;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        try{
            config = new ElasticSearchSinkConnectorConfig(props);
        } catch (ConfigException e){
            throw new ConnectException(e.getMessage(), e);
        }

        esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(config.getString(config.ES_CLUSTER_HOST),
                        config.getInt(config.ES_CLUSTER_PORT)))); // ElasticSearch 에 적재하기 위한 RestHighLevelClient 인스턴스를 생성
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if(records.size() > 0){ // 레코드가 1개 이상으로 들어올경우
            BulkRequest bulkRequest = new BulkRequest(); // 엘라스틱서치로 전송하기 위한 BulkRequest 인스턴스(1개 이상의 데이터들을 묶어놓은 것)를 생성
            for(SinkRecord record : records){
                Gson gson = new Gson();
                Map map = gson.fromJson(record.value().toString(), Map.class);
                bulkRequest.add(new IndexRequest(config.getString(config.ES_INDEX)) // 레코드들을 BulkRequest 에 추가
                        .source(map, XContentType.JSON));
                logger.info("record : {}", record.value());
            }

            // bulkAsync() 메서드로 bulkRequest 에 담은 데이터들을 전송
            // bulkAsync() 메서드를 사용하여 데이터를 전송하고 난 뒤, 결과를 비동기로 받아서 확인가능
            esClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponses) {
                    if(bulkResponses.hasFailures()) {
                        logger.error(bulkResponses.buildFailureMessage()); // 엘라스틱서치에 데이터가 정상적으로 전송되었는지 로그를 남김
                    }
                    else{
                        logger.info("bulk save success"); // bulkAsync() 메서드로 bulkRequest 에 담은 데이터들을 전송
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(e.getMessage(), e); // bulkAsync() 메서드로 bulkRequest 에 담은 데이터들을 전송
                }
            });
        }
    }

    @Override
    public void stop() { // 커넥터가 종료될 경우
        try {
            esClient.close(); // 엘라스틱서치와 연동하는 esClient 변수를 안전하게 종료
        } catch (IOException e){
            logger.info(e.getMessage(), e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets){
        // flush() 메서드가 일정 주기마다 호출되는데,
        // 여기서는 put() 메서드에서 레코드를 받아서 엘라스틱 서치로 전송하므로 flush() 메서드에서 추가로 작성해야할 코드는 없음
        logger.info("flush");
    }
}
