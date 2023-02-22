package com.example.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

// 스트림 프로세서 클래스를 생성하기 위해서는 kafka-streams 라이브러리에서 제공하는 Processor 혹은 Transformer 인터페이스를 사용해야 한다
@Slf4j
public class FilterProcessor implements Processor {

    // 프로세서에 대한 정보를 담고 있는 인스턴스
    // 현재 스트림 중인 토폴로지의 토픽 정보, 애플리케이션 아이디를 조회 가능
    private ProcessorContext context;

    // 실질적으로 프로세싱 로직이 들어가는 부분
    // 1개의 레코드를 받는 것을 가정하여 데이터를 처리
    // forward 메서드를 사용하여 다음 토포로지(다음 프로세서)로 넘어가도록 한다
    // 처리가 완료된 이후에는 commit 을 호출하여 명시적으로 데이터가 처리되었음을 선언
    @Override
    public void process(Record record) {
        String value = (String) record.value();

        if (value.length() > 5) {
            context.forward(record);
        }

        context.commit();
    }

    // 스트림 프로세서의 생성자
    @Override
    public void init(ProcessorContext context) {
        Processor.super.init(context);
        this.context = context;
    }

    // FilterProcessor 가 종료되기 직전에 호출되는 메서드
    // 자원을 해제하는 구문을 추가
    @Override
    public void close() {
        Processor.super.close();
    }
}
