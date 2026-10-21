package com.rutvapatel.pulsedrop.sink;

import com.rutvapatel.pulsedrop.config.AppConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

abstract class BatchingClickHouseSink<T> extends RichSinkFunction<T>
        implements CheckpointedFunction {

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final int batchSize;
    private final long flushMillis;

    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient int pending;
    private transient long lastFlushAt;

    BatchingClickHouseSink(AppConfig config) {
        this.jdbcUrl = config.clickHouseJdbcUrl();
        this.user = config.clickHouseUser();
        this.password = config.clickHousePassword();
        this.batchSize = config.clickHouseBatchSize();
        this.flushMillis = config.clickHouseFlushMillis();
    }

    protected abstract String insertSql();

    protected abstract void bind(PreparedStatement statement, T value) throws SQLException;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        connection = DriverManager.getConnection(jdbcUrl, user, password);
        statement = connection.prepareStatement(insertSql());
        lastFlushAt = System.currentTimeMillis();
    }

    @Override
    public synchronized void invoke(T value, Context context) throws Exception {
        bind(statement, value);
        statement.addBatch();
        pending++;
        long now = System.currentTimeMillis();
        if (pending >= batchSize || now - lastFlushAt >= flushMillis) flush();
    }

    @Override
    public synchronized void snapshotState(FunctionSnapshotContext context) throws Exception {
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // No operator state is required: deterministic keys are deduplicated by ReplacingMergeTree.
    }

    @Override
    public synchronized void close() throws Exception {
        try {
            flush();
        } finally {
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        }
    }

    private void flush() throws SQLException {
        if (pending == 0 || statement == null) return;
        statement.executeBatch();
        pending = 0;
        lastFlushAt = System.currentTimeMillis();
    }
}
