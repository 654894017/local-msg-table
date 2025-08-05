package demo;

import com.damon.localmsgtx.handler.TxMsgKafkaHandler;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;

public class TestKafkaHandler extends TxMsgKafkaHandler<String> {
    public TestKafkaHandler(KafkaConsumer<String, String> kafkaConsumer) {
        super(kafkaConsumer);
    }

    @Override
    public void process(List<String> msgs) {
        System.out.println(msgs);
    }
}
