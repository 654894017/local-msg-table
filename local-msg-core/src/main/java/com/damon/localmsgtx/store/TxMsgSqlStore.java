package com.damon.localmsgtx.store;

import com.damon.localmsgtx.exception.TxMsgDuplicateKeyException;
import com.damon.localmsgtx.exception.TxMsgException;
import com.damon.localmsgtx.exception.TxMsgStoreException;
import com.damon.localmsgtx.model.TxMsgModel;
import com.damon.localmsgtx.model.TxMsgStatusEnum;
import com.damon.localmsgtx.utils.StrUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * 事务消息数据库存储管理器
 * <p>
 * 负责事务消息的持久化（insert/save）、分页查询和过期清理。
 * 采用充血模型设计，所有状态变更由 {@link TxMsgModel} 领域方法完成，
 * 本类只负责持久化，通过 save 方法统一更新（基于乐观锁）。
 */
public class TxMsgSqlStore {

    private final Logger logger = LoggerFactory.getLogger(TxMsgSqlStore.class);

    // ==================== SQL常量 ====================

    private final String INSERT_SQL = "INSERT INTO %s (msg_key, content, topic, msg_tag, status, retry_count, remark, version, create_time, update_time) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final String SAVE_SQL = "UPDATE %s SET status = ?, retry_count = ?, remark = ?, version = version + 1, update_time = ? WHERE id = ? AND version = ?";

    private final String SELECT_WAITING_SQL = "SELECT id, msg_key, content, topic, msg_tag, status, retry_count, remark, version, create_time, update_time " +
            "FROM %s WHERE id > ? AND status IN (?, ?) AND retry_count < ? ORDER BY id ASC LIMIT ?";

    private final String DELETE_EXPIRED_SQL = "DELETE FROM %s WHERE status = ? AND create_time <= ? LIMIT ?";

    private final String CHECK_TABLE_EXISTS_SQL = "SELECT * FROM %s LIMIT 1";

    private final String CREATE_TABLE_SQL = """
            CREATE TABLE `%s` (
              `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
              `content` mediumtext NOT NULL COMMENT '消息内容（JSON格式或字符串）',
              `topic` varchar(255) NOT NULL COMMENT '消息主题',
              `msg_key` varchar(128) NOT NULL COMMENT '消息唯一标识（用于幂等性处理）',
              `msg_tag` varchar(128) NOT NULL COMMENT '消息标签（RocketMQ Tag过滤）',
              `status` tinyint NOT NULL COMMENT '消息状态：0-等待发送，1-已发送，2-发送失败',
              `retry_count` int NOT NULL DEFAULT 0 COMMENT '重试次数',
              `remark` varchar(500) DEFAULT NULL COMMENT '备注（记录发送失败时的错误信息）',
              `version` int NOT NULL DEFAULT 0 COMMENT '版本号（乐观锁）',
              `create_time` bigint NOT NULL COMMENT '创建时间（毫秒时间戳）',
              `update_time` bigint NOT NULL COMMENT '更新时间（毫秒时间戳）',
              PRIMARY KEY (`id`),
              UNIQUE KEY `uk_msgkey` (`msg_key`) USING BTREE COMMENT '消息唯一标识索引',
              KEY `idx_status_createtime` (`status`,`create_time`) USING BTREE COMMENT '状态+时间联合索引（查询和清理使用）'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='事务消息存储表';
            """;

    // ==================== 实例变量 ====================

    private final String tableName;
    private final JdbcTemplate jdbcTemplate;
    private final String topic;

    /**
     * 构造函数
     *
     * @param dataSource 数据源
     * @param tableName  消息存储表名
     * @param topic      消息主题
     */
    public TxMsgSqlStore(DataSource dataSource, String tableName, String topic) {
        Assert.notNull(dataSource, "DataSource cannot be null");
        Assert.hasText(topic, "Topic cannot be empty");
        Assert.hasText(tableName, "Table name cannot be empty");
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.tableName = tableName;
        this.topic = topic;
        initializeTable();
    }

    // ==================== 公开方法 ====================

    /**
     * 插入事务消息（初始状态为等待发送）
     *
     * @param content 消息内容
     * @param msgKey  消息唯一标识
     * @param msgTag  消息标签
     * @return 插入后的消息模型（含自增ID）
     * @throws TxMsgDuplicateKeyException msgKey重复时抛出
     * @throws TxMsgStoreException        持久化失败时抛出
     */
    public TxMsgModel insertTxMsg(String content, String msgKey, String msgTag) {
        Assert.hasText(content, "消息内容不能为空");
        Assert.hasText(msgKey, "消息Key不能为空");

        KeyHolder keyHolder = new GeneratedKeyHolder();
        try {
            jdbcTemplate.update(connection -> {
                PreparedStatement ps = connection.prepareStatement(
                        String.format(INSERT_SQL, tableName),
                        Statement.RETURN_GENERATED_KEYS
                );
                ps.setString(1, msgKey);
                ps.setString(2, content);
                ps.setString(3, topic);
                ps.setString(4, msgTag);
                ps.setInt(5, TxMsgStatusEnum.WAITING.getStatus());
                ps.setInt(6, 0);
                ps.setString(7, StrUtil.EMPTY);
                ps.setInt(8, 0);
                ps.setLong(9, System.currentTimeMillis());
                ps.setLong(10, System.currentTimeMillis());
                return ps;
            }, keyHolder);
            Long id = keyHolder.getKey().longValue();
            logger.debug("事务消息插入成功, id: {}, topic: {}, msgKey: {}", id, topic, msgKey);
            return TxMsgModel.create(id, content, topic, msgKey, msgTag);
        } catch (DuplicateKeyException e) {
            logger.warn("消息Key重复, topic: {}, msgKey: {}", topic, msgKey, e);
            throw new TxMsgDuplicateKeyException("消息Key重复", e);
        } catch (Exception e) {
            logger.error("插入事务消息异常, topic: {}, msgKey: {}", topic, msgKey, e);
            throw new TxMsgStoreException("插入事务消息异常", e);
        }
    }

    /**
     * 保存事务消息状态（乐观锁更新）
     * <p>
     * 根据模型当前的 version 作为 WHERE 条件进行 CAS 更新，
     * 更新成功后数据库 version 自增。模型状态变更应在调用此方法前通过领域方法完成。
     *
     * @param model 已修改状态的事务消息模型
     * @return 受影响行数（0表示版本冲突或记录不存在）
     */
    public int save(TxMsgModel model) {
        Assert.notNull(model, "消息模型不能为空");
        Assert.notNull(model.getId(), "消息ID不能为空");

        try {
            int rows = jdbcTemplate.update(
                    String.format(SAVE_SQL, tableName),
                    model.getStatus(),
                    model.getRetryCount(),
                    model.getRemark(),
                    model.getUpdateTime(),
                    model.getId(),
                    model.getVersion()
            );
            if (rows > 0) {
                // 更新成功，同步内存中的版本号
                model.setVersion(model.getVersion() + 1);
                logger.debug("消息保存成功, id: {}, status: {}, version: {}", model.getId(), model.getStatus(), model.getVersion());
            } else {
                logger.warn("消息保存失败（版本冲突或记录不存在）, id: {}, version: {}", model.getId(), model.getVersion());
            }
            return rows;
        } catch (Exception e) {
            logger.error("保存消息异常, id: {}", model.getId(), e);
            throw new TxMsgStoreException("保存消息异常", e);
        }
    }

    /**
     * 查询待发送的消息列表（分页，基于ID游标）
     *
     * @param pageSize      每页大小
     * @param maxId         上一页最大ID（游标）
     * @param maxRetryCount 最大重试次数过滤条件
     * @return 待发送消息列表
     */
    public List<TxMsgModel> getWaitingMessages(int pageSize, Long maxId, Integer maxRetryCount) {
        Assert.isTrue(pageSize > 0, "页大小必须大于0");

        try {
            return jdbcTemplate.query(
                    String.format(SELECT_WAITING_SQL, tableName),
                    new Object[]{maxId, TxMsgStatusEnum.WAITING.getStatus(), TxMsgStatusEnum.SEND_FAILED.getStatus(), maxRetryCount, pageSize},
                    new TxMsgRowMapper()
            );
        } catch (Exception e) {
            logger.error("查询待发送消息异常, pageSize: {}", pageSize, e);
            throw new TxMsgStoreException("查询待发送消息异常", e);
        }
    }

    /**
     * 批量删除过期消息（循环分批删除，避免大事务）
     *
     * @param expireTime 过期时间戳（毫秒）
     * @param batchSize  每批删除条数
     * @param statusEnum 要删除的消息状态
     */
    public void deleteExpiredSendedMsg(Long expireTime, int batchSize, TxMsgStatusEnum statusEnum) {
        Assert.notNull(expireTime, "过期时间不能为空");
        Assert.isTrue(batchSize > 0, "批次大小必须大于0");

        try {
            int totalDeleted = 0;
            while (true) {
                int deleted = jdbcTemplate.update(
                        String.format(DELETE_EXPIRED_SQL, tableName),
                        statusEnum.getStatus(),
                        expireTime,
                        batchSize
                );
                if (deleted <= 0) {
                    break;
                }
                totalDeleted += deleted;
                logger.info("已删除过期消息 {} 条, 累计: {}", deleted, totalDeleted);
            }
            logger.info("过期消息清理完成, 共删除: {}", totalDeleted);
        } catch (Exception e) {
            logger.error("删除过期消息异常, expireTime: {}", expireTime, e);
            throw new TxMsgStoreException("删除过期消息异常", e);
        }
    }

    // ==================== 私有方法 ====================

    private void initializeTable() {
        try {
            if (!isTableExists()) {
                createTable();
            }
        } catch (Exception e) {
            logger.error("表初始化异常", e);
            throw new TxMsgException("表初始化异常", e);
        }
    }

    private boolean isTableExists() {
        try {
            String checkSql = String.format(CHECK_TABLE_EXISTS_SQL, tableName);
            jdbcTemplate.queryForList(checkSql);
            logger.info("表 {} 已存在", tableName);
            return true;
        } catch (Exception e) {
            logger.info("表 {} 不存在，需要创建", tableName);
            return false;
        }
    }

    private void createTable() {
        try {
            jdbcTemplate.execute(String.format(CREATE_TABLE_SQL, tableName));
            logger.info("成功创建表 {}", tableName);
        } catch (Exception e) {
            logger.error("创建表 {} 失败", tableName, e);
            throw new TxMsgException("创建表失败: " + tableName, e);
        }
    }

    /**
     * 数据库行映射器
     */
    public static class TxMsgRowMapper implements RowMapper<TxMsgModel> {
        @Override
        public TxMsgModel mapRow(ResultSet rs, int rowNum) throws SQLException {
            TxMsgModel model = new TxMsgModel();
            model.setId(rs.getLong("id"));
            model.setMsgKey(rs.getString("msg_key"));
            model.setContent(rs.getString("content"));
            model.setTopic(rs.getString("topic"));
            model.setMsgTag(rs.getString("msg_tag"));
            model.setStatus(rs.getInt("status"));
            model.setRemark(rs.getString("remark"));
            model.setVersion(rs.getInt("version"));
            model.setRetryCount(rs.getInt("retry_count"));
            model.setCreateTime(rs.getLong("create_time"));
            model.setUpdateTime(rs.getLong("update_time"));
            return model;
        }
    }
}

