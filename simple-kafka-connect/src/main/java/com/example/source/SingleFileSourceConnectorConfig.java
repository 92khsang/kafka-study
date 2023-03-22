package com.example.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

@Slf4j
public class SingleFileSourceConnectorConfig extends AbstractConfig {

    // 소스 커넥터는 어떤 파일을 읽을 것인지 지정해야 하므로 파일의 위치와 파일 이름에 대한 정보가 포함됨
    // 옵션명 file 로 파일 위치와 이름을 값으로 받는다
    public static final String DIR_FILE_NAME = "file";
    private static final String DIR_FILE_NAME_DEFAULT_VALUE = "/tmp/kafka.txt";
    private static final String DRI_FILE_NAME_DOC = "읽을 파일 경로와 이름";

    // 읽은 파일을 어느 토픽으로 보낼 것인지 지정하기 위해 옵션명 topic 으로 1개의 토픽값을 받음
    public static final String TOPIC_NAME = "topic";
    private static final String TOPIC_DEFAULT_VALUE = "test";
    private static final String TOPIC_DOC = "보낼 토픽 이름";


    // 커넥터에서 사용할 옵션값들에 대한 정의를 표현하는데 사용
    public static ConfigDef CONFIG = new ConfigDef()
            .define(
                    DIR_FILE_NAME,
                    ConfigDef.Type.STRING,
                    DIR_FILE_NAME_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    DRI_FILE_NAME_DOC
            )
            .define(
                    TOPIC_NAME,
                    ConfigDef.Type.STRING,
                    TOPIC_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    TOPIC_DOC
            );

    public SingleFileSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }

}
