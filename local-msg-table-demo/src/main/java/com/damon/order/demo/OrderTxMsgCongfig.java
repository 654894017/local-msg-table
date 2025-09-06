package com.damon.order.demo;

import com.damon.localmsgtx.client.ITxMsgClient;
import com.damon.localmsgtx.client.KafkaTxMsgClient;
import com.damon.localmsgtx.config.TxMsgKafkaConfig;
import com.damon.localmsgtx.config.factory.TxMsgKafkaConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class OrderTxMsgCongfig {
    public static final String ORDER_TOPIC = "order-events";

    public static final String KAFKA_SERVER = "localhost:9092";

    @Bean
    public ITxMsgClient txMsgClient(DataSource dataSource) {
        TxMsgKafkaConfig config = TxMsgKafkaConfigFactory.simpleConfig(
                KAFKA_SERVER, ORDER_TOPIC,
                dataSource, "transactional_messages"
        );
        return new KafkaTxMsgClient(config);
    }
}