package com.example.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class SingleFileSinkTask extends SinkTask {
    private SingleFileSinkConnectorConfig config;
    private File file;
    private FileWriter fileWriter;


    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            config = new SingleFileSinkConnectorConfig(props);
            file = new File(config.getString(config.DIR_FILE_NAME));
            fileWriter = new FileWriter(file, true);
        } catch (Exception e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    // 데이터를 토픽에서 주기적으로 가져오는 메서드
    // 토픽의 데이터들은 여러 개의 SinkRecord 를 묶어 파라미터로 사용할 수 있다
    // SinkRecord 는 토픽의 한 개 레코드이며, 토픽, 파티션, 타임스테프 등의 정보를 담고 있다
    @Override
    public void put(Collection<SinkRecord> records) {
        try {
            for (SinkRecord record : records) {
                fileWriter.write(record.value().toString() + "\n");
            }
        } catch (IOException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    // put 메서드를 통해 가져온 데이터를 일정 주기로 싱크 어플리케이션 또는 싱크 파일에 저장할 때 사용
    // 예를 들어, JDBC 커넥션을 맺어서 MySQL 에 데이터를 저장할 때 put 메서드에서는 데이터를 insert 하고 flush 메서드는 commit 을 수행하여 트렌잭션을 끝낼 수 있다
    // put 메서드에서 레코드를 저장하는 로직을 넣을 수도 있으며 이 경우에는 flush 메서드를 구현하지 않아도 된다
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        try {
            fileWriter.flush();
        } catch (IOException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public void stop() {
        try {
            fileWriter.close();
        } catch (IOException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }
}
