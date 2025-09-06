package demo;

import com.damon.localmsgtx.client.ITxMsgClient;
import com.damon.localmsgtx.client.KafkaTxMsgClient;
import com.damon.localmsgtx.config.TxMsgKafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.Properties;

@Configuration
public class DemoLocalTxMsgCongfig {

    public final static String topic = "order-events";

    @Bean("producer")
    public KafkaProducer<String, String> producer() {
        // 配置Kafka生产者
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        return new KafkaProducer<>(props);
    }

    @Bean
    public TxMsgKafkaConfig config(
            DataSource dataSource,
            KafkaProducer<String, String> producer) {
        TxMsgKafkaConfig config = new TxMsgKafkaConfig();
        config.setDataSource(dataSource);
        config.setTopic(topic);
        config.setKafkaProducer(producer);
        config.setTxMsgTableName("transactional_messages");
        return config;
    }

    @Bean
    public ITxMsgClient txMsgClient(TxMsgKafkaConfig config) {
        return new KafkaTxMsgClient(config);
    }


}