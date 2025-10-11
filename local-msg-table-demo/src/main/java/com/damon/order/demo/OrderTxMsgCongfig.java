package com.damon.order.demo;

import com.damon.localmsgtx.client.DefaultTxMsgClient;
import com.damon.localmsgtx.client.ITxMsgClient;
import com.damon.localmsgtx.config.TxMsgConfig;
import com.damon.localmsgtx.config.factory.TxMsgKafkaConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class OrderTxMsgCongfig {
    public static final String ORDER_TOPIC = "order-events";

    public static final String KAFKA_SERVER = "localhost:9092";

    public static final String TX_MSG_TABLE = "transactional_messages";

    @Bean
    public ITxMsgClient txMsgClient(DataSource dataSource) {
        TxMsgConfig config = TxMsgKafkaConfigFactory.simpleConfig(
                KAFKA_SERVER,
                ORDER_TOPIC,
                dataSource,
                TX_MSG_TABLE
        );
        return new DefaultTxMsgClient(config);
    }
}
