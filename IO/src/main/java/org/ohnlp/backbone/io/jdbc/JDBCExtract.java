package org.ohnlp.backbone.io.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import jdk.internal.joptsimple.internal.Strings;
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

import java.sql.*;
import java.util.*;

/**
 * Performs data extraction using a JDBC connector
 * This class uses {@link JdbcIO} included with Apache Beam to execute parallelized retrieval queries
 * using offsets for pagination.
 */
public class JDBCExtract extends Extract {
    private int batchSize;
    private JdbcIO.DataSourceConfiguration datasourceConfig;
    private String query;
    private long numBatches;
    private String identifierCol = null;
    private ComboPooledDataSource ds;
    private String[] orderByCols;
    private String viewName;
    private String orderedQuery;

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
     *         "query": "query_to_execute_for_extract_task",
     *         "batch_size": integer_batch_size_per_partition_default_1000_if_blank,
     *         "identifier_col": "column_with_identifier_values"
     *     }
     * </pre>
     *
     * By default, batch_size and identifier_col are optional but are highly recommended as the defaults may
     * not be optimal for performance
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
            // We will first preflight with a query that counts the number of records so that we can get number
            // of batches
            String runId = UUID.randomUUID().toString().replaceAll("-", "_");
            //noinspection SqlResolve
            String countQuery = "SELECT COUNT(*) FROM (" + query + ") bckbone_preflight_query_" + runId;
            this.viewName = "backbone_jdbcextract_" + runId;
            // Find appropriate columns to order by so that pagination results are consistent
            this.orderByCols = findPaginationOrderingColumns(this.query);
            // Get record count so that we know how many batches are going to be needed
            try (Connection conn = ds.getConnection()) {
                ResultSet rs = conn.createStatement().executeQuery(countQuery);
                rs.next();
                int resultCount = rs.getInt(1);
                this.numBatches = Math.round(Math.ceil((double)resultCount/this.batchSize));
            }
            this.orderedQuery = "SELECT * FROM (" + this.query + ") " + this.viewName
                    + " ORDER BY " + Strings.join(this.orderByCols, ", ") + " ";
            // Now we have to add the offset/fetch in the dialect local format..
            // Specifically, postgres and MySQL are special in that they do not conform to the
            // SQL:2011 standard syntax
            if (driver.equals("org.postgresql.Driver") || driver.equals("com.mysql.jdbc.Driver")) {
                this.orderedQuery += "LIMIT " + batchSize + " OFFSET ?";
            } else { // This is the SQL:2011 standard definition of an offset...fetch syntax
                this.orderedQuery += "OFFSET ? ROWS FETCH NEXT " + batchSize + " ROWS ONLY";
            }
        } catch (Throwable t) {
            throw new ComponentInitializationException(t);
        }
    }

    private String[] findPaginationOrderingColumns(String query) throws ComponentInitializationException {
        try (Connection conn = this.ds.getConnection()) {
            ResultSetMetaData queryMeta = conn.prepareStatement(query).getMetaData();
            Map<String, Integer> colNameToIndex = new HashMap<>();
            for (int i = 0; i < queryMeta.getColumnCount(); i++) {
                // Assume we are using a case-sensitive impl:
                // config mismatches can be addressed via config change for identifierCol but same is not true if
                // impl is case-sensitive and we try to use a case-normalized column name via automatic selection
                colNameToIndex.put(queryMeta.getColumnLabel(i), i);
            }
            if (this.identifierCol != null) {
                // User-supplied identifier column exists, make sure it actually exists in query results
                if (!colNameToIndex.containsKey(this.identifierCol)) {
                    throw new ComponentInitializationException(
                            new IllegalArgumentException("The supplied identifier_col " + this.identifierCol + " " +
                            "does not exist in the returned query results. Available columns: " +
                            Arrays.toString(colNameToIndex.keySet().toArray(new String[0]))));
                }
                return new String[] {this.identifierCol};
            } else {
                // User did not supply an identifier column, so instead we will concatenate by column index so that
                // ordering is done for all columns...
                // Ideally we would want to instead look for a pk/unique constraint and order on that instead of on
                // everything
                List<String> colNames = new ArrayList<>();
                for (int i = 0; i < queryMeta.getColumnCount(); i++) {
                    colNames.add(queryMeta.getColumnLabel(i));
                }
                return colNames.toArray(new String[0]);
            }
        } catch (SQLException t) {
            throw new ComponentInitializationException(t);
        }

    }

    public PCollection<Row> expand(PBegin input) {
        List<Integer> offsets = new ArrayList<>();
        for (int i = 0; i < numBatches; i++) {
            offsets.add(i * batchSize); // Create a sequence of batches at the appropriate offset
        }
        Schema schema;
        try (Connection conn = ds.getConnection();
                PreparedStatement ps = conn.prepareStatement(this.query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            schema = SchemaUtilProxy.toBeamSchema(ps.getMetaData());
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
        return input.apply("JDBC Preflight", Create.of(offsets)) // First create partitions # = to num batches
                .apply("JDBC Read", // Now actually do the read, the readall function will execute one query per input partition
                        JdbcIO.<Integer, Row>readAll()
                                .withDataSourceConfiguration(datasourceConfig)
                                .withQuery(this.orderedQuery)
                        .withRowMapper(new SchemaUtilProxy.BeamRowMapperProxy(schema))
                        .withParameterSetter((JdbcIO.PreparedStatementSetter<Integer>) (element, preparedStatement) -> {
                            preparedStatement.setInt(1, element); // Replace
                        })
                );
    }
}
