package com.example.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class SingleFileSinkConnectorConfig extends AbstractConfig {

    public static final String DIR_FILE_NAME = "file";
    private static final String DIR_FILE_NAME_DEFAULT_VALUE = "/tmp/kafka.txt";
    private static final String DRI_FILE_NAME_DOC = "저장할 디렉토리와 파일 이름";

    // 토픽이 옶션값에 없는 이유는 커넥터를 통해 커넥터를 싱행 시 기본값으로 받아야 하기 떄문이다

    public static ConfigDef CONFIG = new ConfigDef()
            .define(
                    DIR_FILE_NAME,
                    ConfigDef.Type.STRING,
                    DIR_FILE_NAME_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    DRI_FILE_NAME_DOC
            );

    public SingleFileSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }
}
