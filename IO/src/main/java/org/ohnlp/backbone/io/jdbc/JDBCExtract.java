package org.ohnlp.backbone.io.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.SchemaUtilProxy;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Performs data extraction using a JDBC connector
 * This class is essentially a configuration wrapper around the beam provided {@link JdbcIO} transform
 */
public class JDBCExtract extends Extract {
    private int batchSize;
    private String cursorName;
    private JdbcIO.DataSourceConfiguration datasourceConfig;
    private String query;
    private long numBatches;
    private ComboPooledDataSource ds;

    /**
     * Initializes a Beam JdbcIO Provider
     *
     * <p>
     * Expected configuration structure:
     * <pre>
     *     {
     *         "url": "jdbc_url_to_database",
     *         "driver": "jdbc.driver.class",
     *         "user": "dbUsername",
     *         "password": "dbPassword",
     *         "query": "query_to_execute_for_extract_task"
     *     }
     * </pre>
     *
     * @param config The configuration section pertaining to this component
     * @throws ComponentInitializationException if an error occurs during initialization or if configuraiton contains
     *                                          unexpected values
     */
    public void initFromConfig(JsonNode config) throws ComponentInitializationException {
        try {
            String url = config.get("url").asText();
            String driver = config.get("driver").asText();
            String user = config.get("user").asText();
            String password = config.get("password").asText();
            this.query = config.get("query").asText();
            this.batchSize = config.has("batch_size") ? config.get("batch_size").asInt() : 1000;
            this.ds = new ComboPooledDataSource();
            ds.setDriverClass(driver);
            ds.setJdbcUrl(url);
            ds.setUser(user);
            ds.setPassword(password);
            this.datasourceConfig = JdbcIO.DataSourceConfiguration
                    .create(ds);
            // We will first preflight with a query that creates a cursor for pagination purposes, with a hold for cross-transaction
            String runId = UUID.randomUUID().toString().replaceAll("-", "_");
            //noinspection SqlResolve
            this.cursorName = "backbone_jdbcio_cursor_" + runId;
            String cursorQuery = "DECLARE " + cursorName + " WITH HOLD FOR " + query;
            String countQuery = "SELECT COUNT(*) FROM (" + query + ") bckbone_preflight_query_" + runId;
            try (Connection conn = ds.getConnection()) {
                // Ensure count and cursor queries occur in same
                conn.setAutoCommit(false);
                ResultSet rs = conn.createStatement().executeQuery(countQuery);
                rs.next();
                int resultCount = rs.getInt(1);
                conn.createStatement().executeQuery(cursorQuery);
                this.numBatches = Math.round(Math.ceil((double)resultCount/this.batchSize));
                conn.commit();
            }
            ds.getConnection().createStatement().execute(cursorQuery);
        } catch (Throwable t) {
            throw new ComponentInitializationException(t);
        }
    }

    public PCollection<Row> expand(PBegin input) {
        List<KV<Integer, Integer>> queries = new ArrayList<>();
        for (int i = 0; i < numBatches; i++) {
            queries.add(KV.of(batchSize, 1)); // Create a sequence of batches with batchsize in parameter 1
        }
        Schema schema;
        try (Connection conn = ds.getConnection();
                PreparedStatement ps = conn.prepareStatement(this.query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            schema = SchemaUtilProxy.toBeamSchema(ps.getMetaData());
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
        return input.apply("JDBC Preflight", Create.of(queries)) // First create partitions # = to num batches
                .apply("JDBC Read", // Now actually do the read, the readall function will execute one query per input partition
                        JdbcIO.<KV<Integer, Integer>, Row>readAll()
                                .withDataSourceConfiguration(datasourceConfig)
                                .withQuery("FETCH ? FROM " + cursorName)
                        .withRowMapper(new SchemaUtilProxy.BeamRowMapperProxy(schema))
                        .withParameterSetter((JdbcIO.PreparedStatementSetter<KV<Integer, Integer>>) (element, preparedStatement) -> {
                            preparedStatement.setInt(1, batchSize); // We don't actually care about contents
                        })
                );
    }
}
