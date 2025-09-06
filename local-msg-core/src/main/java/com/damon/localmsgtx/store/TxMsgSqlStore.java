package com.damon.localmsgtx.store;

import com.damon.localmsgtx.exception.TxMsgDuplicateKeyException;
import com.damon.localmsgtx.exception.TxMsgException;
import com.damon.localmsgtx.model.TxMsgModel;
import com.damon.localmsgtx.model.TxMsgStatusEnum;
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
 * Transactional message database storage manager
 * Responsible for persistence, status updates, queries, and cleanup of transactional messages
 */
public class TxMsgSqlStore {

    private final Logger logger = LoggerFactory.getLogger(TxMsgSqlStore.class);
    // SQL语句常量
    private final String INSERT_TX_MSG_SQL = "INSERT INTO %s (msg_key, content, topic, status, create_time, update_time ) " +
            "VALUES (?, ?, ?, ?, ?, ?)";
    private final String UPDATE_SEND_MSG_SQL = "UPDATE %s SET status = ?, update_time = ? WHERE id = ?";
    private final String SELECT_WAITING_MSG_SQL = "SELECT id, msg_key, content, topic, status, create_time, update_time " +
            "FROM %s WHERE status = ? ORDER BY create_time ASC LIMIT ?";
    private final String DELETE_EXPIRED_SENDED_MSG_SQL = "DELETE FROM %s WHERE status = ? AND create_time <= ? LIMIT ?";
    private final String CHECK_TABLE_EXISTS_SQL = "SELECT * FROM %s LIMIT 1";
    private final String CREATE_TABLE_SQL = """
            CREATE TABLE `%s` (
              `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
              `content` text NOT NULL COMMENT '消息内容（JSON格式或字符串）',
              `topic` varchar(255) NOT NULL COMMENT 'Kafka消息主题',
              `msg_key` varchar(128) NOT NULL COMMENT '消息唯一标识（用于幂等性处理）',
              `status` tinyint NOT NULL COMMENT '消息状态：0-等待发送，1-已发送',
              `create_time` bigint NOT NULL COMMENT '创建时间（毫秒时间戳）',
              `update_time` bigint NOT NULL COMMENT '更新时间（毫秒时间戳）',
              PRIMARY KEY (`id`),
              UNIQUE KEY `uk_msg_key` (`msg_key`) USING BTREE COMMENT '用于根据msgKey查询消息（可选，根据业务需求添加）',
              KEY `idx_status_create_time` (`status`,`create_time`) COMMENT '用于查询等待发送的消息和清理过期消息'
            ) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='事务消息存储表（确保消息发送与本地事务一致性）';
            """;
    /**
     * Database table name (default: mq_messages)
     */
    private final String tableName;
    private final JdbcTemplate jdbcTemplate;
    private final String topic;

    /**
     * Constructor (supports custom table name)
     *
     * @param dataSource Data source
     * @param tableName  Message storage table name
     */
    public TxMsgSqlStore(DataSource dataSource, String tableName, String topic) {
        Assert.notNull(dataSource, "Data source cannot be null");
        Assert.hasText(topic, "topic cannot be empty");
        Assert.hasText(tableName, "Table name cannot be empty");
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.tableName = tableName;
        this.topic = topic;
        initializeTable();
    }

    /**
     * Check and create table (if not exists)
     */
    private void initializeTable() {
        try {
            // Check if table exists
            if (!isTableExists()) {
                // Create table if it does not exist
                createTable();
            }
        } catch (Exception e) {
            logger.error("Exception occurred during table initialization", e);
            throw new TxMsgException("Exception occurred during table initialization", e);
        }
    }

    /**
     * Check if table exists
     *
     * @return Whether table exists
     */
    private boolean isTableExists() {
        try {
            // Use a simple query to detect if table exists
            String checkSql = String.format(CHECK_TABLE_EXISTS_SQL, tableName);
            jdbcTemplate.queryForList(checkSql);
            logger.info("Table {} already exists", tableName);
            return true;
        } catch (Exception e) {
            logger.info("Table {} does not exist, needs to be created", tableName);
            return false;
        }
    }

    /**
     * Create table
     */
    private void createTable() {
        try {
            jdbcTemplate.execute(String.format(CREATE_TABLE_SQL, tableName));
            logger.info("Successfully created table {}", tableName);
        } catch (Exception e) {
            logger.error("Failed to create table {}", tableName, e);
            throw new TxMsgException("Failed to create table: " + tableName, e);
        }
    }

    /**
     * Insert transactional message (status is waiting to send)
     *
     * @param content Message content
     * @param msgKey  Message unique identifier
     * @return Inserted message model
     */
    public TxMsgModel insertTxMsg(String content, String msgKey) {
        // Parameter validation
        Assert.hasText(content, "Message content cannot be empty");
        Assert.hasText(msgKey, "Message key cannot be empty");

        KeyHolder keyHolder = new GeneratedKeyHolder();
        long currentTime = System.currentTimeMillis();

        try {
            jdbcTemplate.update(connection -> {
                PreparedStatement ps = connection.prepareStatement(String.format(INSERT_TX_MSG_SQL, tableName), Statement.RETURN_GENERATED_KEYS);
                ps.setString(1, msgKey);
                ps.setString(2, content);
                ps.setString(3, topic);
                ps.setInt(4, TxMsgStatusEnum.WAITING.getStatus());
                ps.setLong(5, currentTime);
                ps.setLong(6, currentTime);
                return ps;
            }, keyHolder);
            Long id = keyHolder.getKey().longValue();
            logger.debug("Transactional message inserted successfully, id: {}, topic: {}, msgKey: {}", id, topic, msgKey);
            return buildTxMsgModel(id, content, topic, msgKey, TxMsgStatusEnum.WAITING.getStatus(), currentTime);
        } catch (DuplicateKeyException e) {
            logger.warn("Duplicate key exception occurred while inserting transactional message, topic: {}, msgKey: {}", topic, msgKey, e);
            throw new TxMsgDuplicateKeyException("Duplicate key exception occurred while inserting transactional message", e);
        } catch (Exception e) {
            logger.error("Exception occurred while inserting transactional message, topic: {}, msgKey: {}", topic, msgKey, e);
            throw new TxMsgException("Exception occurred while inserting transactional message", e);
        }
    }

    /**
     * Update message status to sent
     *
     * @param txMsgModel Message model
     * @return Number of affected rows
     */
    public int updateSendMsg(TxMsgModel txMsgModel) {
        Assert.notNull(txMsgModel, "Message model cannot be null");
        Assert.notNull(txMsgModel.getId(), "Message ID cannot be null");

        try {
            int rows = jdbcTemplate.update(
                    String.format(UPDATE_SEND_MSG_SQL, tableName),
                    TxMsgStatusEnum.SENT.getStatus(),
                    System.currentTimeMillis(),
                    txMsgModel.getId()
            );
            if (rows > 0) {
                logger.debug("Message status updated to sent, id: {}", txMsgModel.getId());
            } else {
                logger.warn("Failed to update message status, corresponding record not found, id: {}", txMsgModel.getId());
            }
            return rows;
        } catch (Exception e) {
            logger.error("Exception occurred while updating message status, id: {}", txMsgModel.getId(), e);
            throw new TxMsgException("Exception occurred while updating message status", e);
        }
    }

    /**
     * Get list of messages waiting to send
     *
     * @param pageSize Page size
     * @return List of messages waiting to send
     */
    public List<TxMsgModel> getWaitingMessages(int pageSize) {
        Assert.isTrue(pageSize > 0, "Page size must be greater than 0");

        try {
            return jdbcTemplate.query(
                    String.format(SELECT_WAITING_MSG_SQL, tableName),
                    new Object[]{TxMsgStatusEnum.WAITING.getStatus(), pageSize},
                    new TxMsgRowMapper()
            );
        } catch (Exception e) {
            logger.error("Exception occurred while querying messages waiting to send, pageSize: {}", pageSize, e);
            throw new TxMsgException("Exception occurred while querying messages waiting to send", e);
        }
    }

    /**
     * Delete sent messages that exceed the specified time (batch deletion to avoid large transactions)
     *
     * @param expireTime Expiration time (millisecond timestamp, messages less than or equal to this time will be deleted)
     * @param batchSize  Batch size for each deletion
     */
    public void deleteExpiredSendedMsg(Long expireTime, int batchSize, TxMsgStatusEnum statusEnum) {
        Assert.notNull(expireTime, "Expiration time cannot be null");
        Assert.isTrue(batchSize > 0, "Batch size must be greater than 0");

        try {
            int totalDeleted = 0;
            while (true) {
                int deleted = jdbcTemplate.update(
                        String.format(DELETE_EXPIRED_SENDED_MSG_SQL, tableName), statusEnum.getStatus(), expireTime, batchSize
                );
                if (deleted <= 0) {
                    break;
                }
                totalDeleted += deleted;
                logger.info("Deleted {} expired messages, total deleted: {}", deleted, totalDeleted);
            }
            logger.info("Expired message cleanup completed, total deleted: {}", totalDeleted);
        } catch (Exception e) {
            logger.error("Exception occurred while deleting expired messages, expireTime: {}", expireTime, e);
            throw new TxMsgException("Exception occurred while deleting expired messages", e);
        }
    }

    /**
     * Build message model
     */
    private TxMsgModel buildTxMsgModel(Long id, String content, String topic, String msgKey, int status, long createTime) {
        TxMsgModel model = new TxMsgModel();
        model.setId(id);
        model.setContent(content);
        model.setTopic(topic);
        model.setMsgKey(msgKey);
        model.setStatus(status);
        model.setCreateTime(createTime);
        model.setUpdateTime(createTime);
        return model;
    }

    public int batchUpdateSendMsg(List<Long> successMsgIds) {
        Assert.notNull(successMsgIds, "Message ID list cannot be null");
        try {
            // Build batch update SQL, use IN clause to update status of multiple IDs
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("UPDATE ").append(tableName)
                    .append(" SET status = ?, update_time = ?")
                    .append(" WHERE id IN (");

            // Add placeholders for each ID
            for (int i = 0; i < successMsgIds.size(); i++) {
                sqlBuilder.append("?");
                if (i < successMsgIds.size() - 1) {
                    sqlBuilder.append(",");
                }
            }
            sqlBuilder.append(")");

            String sql = sqlBuilder.toString();

            // Prepare parameter array
            Object[] params = new Object[2 + successMsgIds.size()];
            params[0] = TxMsgStatusEnum.SENT.getStatus();
            params[1] = System.currentTimeMillis();

            for (int i = 0; i < successMsgIds.size(); i++) {
                params[2 + i] = successMsgIds.get(i);
            }

            int updatedRows = jdbcTemplate.update(sql, params);
            logger.debug("Batch update of message status successful, updated records: {}, message ID list: {}", updatedRows, successMsgIds);
            return updatedRows;
        } catch (Exception e) {
            logger.error("Exception occurred during batch update of message status, message ID list: {}", successMsgIds, e);
            throw new TxMsgException("Exception occurred during batch update of message status", e);
        }
    }

    public static class TxMsgRowMapper implements RowMapper<TxMsgModel> {
        @Override
        public TxMsgModel mapRow(ResultSet rs, int rowNum) throws SQLException {
            TxMsgModel model = new TxMsgModel();
            model.setId(rs.getLong("id"));
            model.setMsgKey(rs.getString("msg_key"));
            model.setContent(rs.getString("content"));
            model.setTopic(rs.getString("topic"));
            model.setStatus(rs.getInt("status"));
            model.setCreateTime(rs.getLong("create_time"));
            model.setUpdateTime(rs.getLong("update_time"));
            return model;
        }
    }
}