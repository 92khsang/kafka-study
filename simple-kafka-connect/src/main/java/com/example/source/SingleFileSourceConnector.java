package com.example.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


// SingleFileSourceConnector 는 커넥터에서 사용할 커넥터의 이름이 된다
// 플러그인으로 추가하여 사용 시에는 패키지 이름과 함께 붙여서 사용된다.
@Slf4j
public class SingleFileSourceConnector extends SourceConnector {

    private Map<String, String> configProperties;

    // 커넥터 버전을 리턴
    // 커넥트에 포함된 커넥터 플러그인을 조회할 때 이 버전이 노출된다
    // 커넥트를 지속적으로 유지보수하고 신규 배포할 때 이 메서드가 리턴하는 버전값을 변경해야 한다
    @Override
    public String version() {
        return "1.0";
    }

    // 사용자가 JSON 또는 config 파일 형태로 입력한 설정값을 초기화하는 메서드
    // 만약 올바른 값이 아니라면 여기서 ConnectException() 을 호출하여 커넥터를 종류할 수 있다.
    // 예를 들어, JDBC 커넥터라면 JDBC 커넥터 URL 값을 검증하는 로직을 넣을 수 있다
    @Override
    public void start(Map<String, String> props) {
        this.configProperties = props;

        try {
            // 커넥트에서 SingleFileSourceConnector 를 생성할 때 받은 설정값들을 초기화
            // 설정을 초기화할 때 필수 설정값이 빠져있다면 ConnectException 을 발생시켜 커넥터를 종료
            new SingleFileSourceConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    // 이 커넥터가 사용하는 테스크 클랙스를 지정
    @Override
    public Class<? extends Task> taskClass() {
        return SingleFileSourceTask.class;
    }


    // 태스크 개수가 2개 이상인 경우 테스크마다 각기 다른 옵션을 설정할 때 사용
    // 여기선 모두 동일한 옵션을 사용하도록 설정
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    // 커넥터가 사용할 설정값에 대한 정보를 받는다
    // 커넥터의 설정값은 ConfigDef 클래스를 통해 각 설정의 이름, 기본값, 중요도, 설명을 정의할 수 있다
    @Override
    public ConfigDef config() {
        return SingleFileSourceConnectorConfig.CONFIG;
    }

    // 커넥터가 종료될 때 필요한 로직을 작성한다
    @Override
    public void stop() {
    }
}
