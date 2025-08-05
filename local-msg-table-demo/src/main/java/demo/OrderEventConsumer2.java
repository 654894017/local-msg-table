package demo;

import com.damon.localmsgtx.client.ITxFeedbackMsgClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Properties;

@Component
public class OrderEventConsumer2 {
    @Autowired
    private ITxFeedbackMsgClient txFeedbackMsgClient;

    public OrderEventConsumer2() {
        // 在后台线程中启动消费者
        new Thread(this::consumeMessages).start();
    }

    private void consumeMessages() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-event-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("order-events"));
        System.out.println("Kafka consumer started, listening to 'order-events' topic");

        TestKafkaHandler testConsumer = new TestKafkaHandler(consumer);
        testConsumer.run();
    }
}