package com.example.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// 소스 테스크에서 중요한 점은 소스 파일로부터 읽고 토픽으로 보낸 지점을 기록하고 사용한다는 점이다
// 소스 파일에서 마지막으로 읽은 지점을 기록하지 않으면 재시작했을 때 데이터가 중복해서 보내질 수 있다
// 소스 테스크에서는 마지막 지점을 저장하기 위해 오프셋 스토리지에 데이터를 저장한다
// 테스크를 시작할 때 오프셋 스토리지에서 마지막으로 읽어온 지점을 가져오고, 데이터를 보냈을 때는 오프셋 스토리지에서 마지막으로 읽은 지점을 담은 데이터를 저장한다
@Slf4j
public class SingleFileSourceTask extends SourceTask {

    // 파일 이름과 해당 파일을 읽은 지점을 오프셋 스토리지에 저장하기 위해 filename 과 position 값을 정의
    // 이 2개의 키를 기준으로 오프셋 스토리지에 읽은 위치를 저장한다
    private final String FILENAME_FIELD = "filename";
    private final String POSITION_FILED = "position";

    // 오프셋 스토리지에 데이터를 저장하고 읽을 때는 Map 자료구조에 담은 데이터를 사용한다
    // "filename" 이 키, 커넥터가 읽는 파일 이름이 값으로 저장되어 사용된다
    private Map<String, String> fileNamePartition;
    private Map<String, Object> offset;

    private String topic;
    private String file;

    // 읽은 파일의 위치를 커넥터 멤버 변수로 지정하여 사용한다
    // 커넥터가 최초로 실행될 때, 오프셋 스토리지에 마지막으로 읽은 파일의 위치를 position 변수에 선언하여 중복 적재되지 않도록 할 수 있다.
    // 만약, 처음 읽는 파일이라면 오프셋 스토리지에 해당 파일을 읽은 기록이 없으므로 position 은 0 으로 설정하여 처음부터 읽도록 한다
    private long position = -1;

    // 테스크 버전을 지정
    // 보통 커넥터와 동일한 버전을 사용한다
    @Override
    public String version() {
        return "1.0";
    }

    // 테스크가 시작할 때 필요한 로직을 작성
    // 테스크는 실질적으로 데이터를 처리하는 역할을 하므로 데이터 처리에 필요한 모든 리소스를 여기서 초기화
    // 예를 들어, JDBC 소스 커넥터를 구현한다면 이 메서드에서 JDBC 커넥션을 맺는다
    @Override
    public void start(Map<String, String> props) {
        try {
            // 커넥터 실행 시 받은 설정값을 SingleFileSourceConnectorConfig 로 선언하여 사용
            // 여기서는 토픽 이름과 읽을 파일 이름 설정값을 사용한다
            SingleFileSourceConnectorConfig config = new SingleFileSourceConnectorConfig(props);
            topic = config.getString(SingleFileSourceConnectorConfig.TOPIC_NAME);
            file = config.getString(SingleFileSourceConnectorConfig.DIR_FILE_NAME);
            fileNamePartition = Collections.singletonMap(FILENAME_FIELD, file);

            // 오프셋 스토리지에서 읽고자 하는 파일 정보를 가져온다
            // 오프셋 스토리지는 실제로 데이터가 저장되는 곳으로 단일 모드 커넥트는 로컬 파일로 저장하고, 분산 모드 커넥트는 내부 토픽으로 저장한다
            offset = context.offsetStorageReader().offset(fileNamePartition);


            // offset 이 null 이 아니면 한 번이라도 커넥터를 통해 해당 파일을 처리했다는 의미
            if (offset != null) {
                // 해당 파일에 대한 정보가 있을 경우에는 파일의 마지막 읽은 위치를 get() 메서드를 통해 가져온다
                Object lastReadFileOffset = offset.get(POSITION_FILED);
                if (lastReadFileOffset != null) {
                    // 오프셋 스토리지에 가져온 마지막 처리한 지점을 position 변수에 할당
                    // 이 작업을 통해 커넥터가 재시작되더라도 데이터의 중복, 유실 처리를 막을 수 있다
                    position = (Long) lastReadFileOffset;
                }
            } else { // 만약 오프셋 스토리지에서 데이터를 읽었을 때 null 이 반환되면 읽고자 하는 데이터가 없다는 의미
                position = 0;
            }
        } catch (Exception e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    // 소스 애플리케이션 또는 소스 파일로부터 데이터를 읽어오는 로직을 작성
    // 데이터를 읽어오면 토피으로 보낼 데이터를 SourceRecord 로 정의
    // SourceRecord 클래스는 토픽으로 데이터를 정의하기 위해 사용
    // List<SourceRecord> 인스턴스에 데이터를 담아 리턴하면 데이터가 토픽으로 전송된다

    // 테스크가 시작한 이후 지속적으로 데이터를 가져오기 위해 반복적으로 호출되는 메서드
    // 소스 파일에서 데이터를 읽어서 토픽으로 데이터를 보내야 한다
    // 토픽으로 데이터를 보내는 방법은 List<SourceRecord> 를 리턴하는 것
    // SourceRecord 는 토픽으로 보낼 데이터를 담는 클래스
    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = new ArrayList<>();
        try {
            Thread.sleep(1000);


            List<String> lines = getLines(position);

            if (lines.size() > 0) {
                lines.forEach(line -> {
                    // 마지막 전송한 데이터의 위치를 오프셋 스토리지에 저장하기 위해 앞서 선언한 fileNamePartition 과 현재 토픽으로 보내는 줄의 위치를 기록한 sourceOffset 을 파라미터로 넣어 선언한다
                    Map<String, Long> sourceOffset = Collections.singletonMap(POSITION_FILED, ++position);
                    SourceRecord record = new SourceRecord(fileNamePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line);
                    records.add(record);
                });
            }
            return records;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new ConnectException(e.getMessage(), e);
        }
    }

    private List<String> getLines(long readLine) throws Exception {
        BufferedReader reader = Files.newBufferedReader(Paths.get(file));
        return reader.lines().skip(readLine).collect(Collectors.toList());
    }

    // 테스크가 종료될 떄 필요한 로직을 작성
    // JDBC 소스 커넥터를 구현했다면 이 메서드에서 JDBC 커넥션을 종료하는 로직을 추가하면 된다
    @Override
    public void stop() {

    }
}
