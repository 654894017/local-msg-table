package demo.config;

import com.damon.localmsgtx.ITxMsgClient;
import com.damon.localmsgtx.TxMsgKafkaConfig;
import com.damon.localmsgtx.impl.KafkaTxMsgClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.Properties;

@Configuration
public class DemoLocalTxmsgCongfig {

    @Bean
    public KafkaProducer<String, String> producer() {
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
    public TxMsgKafkaConfig config(DataSource dataSource, KafkaProducer<String, String> kafkaProducer) {
        TxMsgKafkaConfig config = new TxMsgKafkaConfig();
        config.setDataSource(dataSource);
        config.setKafkaProducer(kafkaProducer);
        config.setTxMsgTableName("transactional_messages");
        return config;
    }

    @Bean
    public ITxMsgClient txMsgClient(TxMsgKafkaConfig config) {
        return new KafkaTxMsgClient(config);
    }


}