package demo.config;

import com.damon.localmsgtx.client.ITxFeedbackMsgClient;
import com.damon.localmsgtx.client.impl.KafkaTxFeedbackMsgClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class DemoTxFeedbackMsgCongfig {
    @Bean("feedbackProducer")
    public KafkaProducer<String, String> feedbackProducer() {
        // 配置Kafka生产者
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        return kafkaProducer;
    }

    @Bean
    public ITxFeedbackMsgClient txFeedbackMsgClient(KafkaProducer<String, String> feedbackProducer) {
        return new KafkaTxFeedbackMsgClient(feedbackProducer, DemoLocalTxMsgCongfig.feedbackTopic);
    }


}