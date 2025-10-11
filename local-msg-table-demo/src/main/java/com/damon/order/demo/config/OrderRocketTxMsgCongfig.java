package com.damon.order.demo.config;

import com.damon.localmsgtx.client.DefaultTxMsgClient;
import com.damon.localmsgtx.client.ITxMsgClient;
import com.damon.localmsgtx.config.TxMsgConfig;
import com.damon.localmsgtx.config.factory.TxMsgRocketConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class OrderRocketTxMsgCongfig {
    public static final String ORDER_TOPIC = "order-events";

    public static final String ROCKETMQ_SERVER = "localhost:9876";

    public static final String ORDER_GROUP = "order_group";

    public static final String ROCKET_TX_MSG_TABLE = "rocket_transactional_messages";

    @Bean("rocketTxMsgClient")
    public ITxMsgClient txMsgClient(DataSource dataSource) {
        TxMsgConfig config = TxMsgRocketConfigFactory.simpleConfig(
                ROCKETMQ_SERVER,
                ORDER_TOPIC,
                ORDER_GROUP,
                dataSource,
                ROCKET_TX_MSG_TABLE
        );
        return new DefaultTxMsgClient(config);
    }
}