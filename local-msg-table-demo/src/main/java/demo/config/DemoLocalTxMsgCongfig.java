package demo.config;

import com.damon.localmsgtx.client.ITxMsgClient;
import com.damon.localmsgtx.client.impl.KafkaTxMsgClient;
import com.damon.localmsgtx.config.TxMsgKafkaConfig;
import com.damon.localmsgtx.feedback.TxFeedbackMsgDispatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.Properties;

@Configuration
public class DemoLocalTxMsgCongfig {

    public final static String topic = "order-events";

    public final static String feedbackTopic = topic + "-feedback";

    @Bean("producer")
    public KafkaProducer<String, String> producer() {
        // 配置Kafka生产者
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        return new KafkaProducer<>(props);
    }

    @Bean("feedbackConsumer")
    public KafkaConsumer<String, String> feedbackConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, feedbackTopic + "-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(feedbackTopic));
        return consumer;
    }

    @Bean
    public TxMsgKafkaConfig config(
            DataSource dataSource,
            KafkaProducer<String, String> producer,
            KafkaConsumer<String, String> feedbackConsumer
    ) {
        TxMsgKafkaConfig config = new TxMsgKafkaConfig();
        config.setDataSource(dataSource);
        config.setTopic(topic);
        config.setFeedbackTopic(feedbackTopic);
        config.setKafkaProducer(producer);
        config.setTxMsgTableName("transactional_messages");
        config.setEnableFeedbackMsgConsumer(true);
        config.setKafkaConsumer(feedbackConsumer);
        return config;
    }

    @Bean
    public ITxMsgClient txMsgClient(TxMsgKafkaConfig config) {
        return new KafkaTxMsgClient(config);
    }

    @Bean
    public TxFeedbackMsgDispatch txFeedbackMsgDispatch(TxMsgKafkaConfig config) {
        int concurrentThreadNumber = config.isEnableFeedbackMsgConsumer() ? 1 : 0;
        return new TxFeedbackMsgDispatch(config.getKafkaConsumer(), concurrentThreadNumber, config.getTxMsgSqlStore());
    }


}