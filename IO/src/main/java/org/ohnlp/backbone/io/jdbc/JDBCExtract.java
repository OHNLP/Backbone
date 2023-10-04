package org.ohnlp.backbone.io.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CollectionCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.SchemaUtilProxy;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.components.ExtractToOne;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.backbone.io.Repartition;

import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Performs data extraction using a JDBC connector
 * This class uses {@link JdbcIO} included with Apache Beam to execute parallelized retrieval queries
 * using offsets for pagination.
 */
@ComponentDescription(
        name = "Read Records from JDBC-compatible data source",
        desc = "Reads Records from a JDBC-compatible data source using a SQL query. Any queries should ideally " +
                "include an indexed identifier column that can be used to rapidly paginate/partition results for " +
                "parallelized processing"
)
public class JDBCExtract extends ExtractToOne {
    @ConfigurationProperty(
            path = "url",
            desc = "The JDBC URL to connect to"
    )
    private String url;
    @ConfigurationProperty(
            path = "driver",
            desc = "The JDBC driver to use for the connection"
    )
    private String driver;
    @ConfigurationProperty(
            path = "user",
            desc = "Database User"
    )
    private String user;
    @ConfigurationProperty(
            path = "password",
            desc = "Database Password"
    )
    private String password;
    @ConfigurationProperty(
            path = "query",
            desc = "Database Query to Execute"
    )
    private String query;
    @ConfigurationProperty(
            path = "batch_size",
            desc = "Approximate number of documents per batch/partition. Lower this if running into memory issues.",
            required = false
    )
    private int batchSize = 1000;
    @ConfigurationProperty(
            path = "identifier_col",
            desc = "An ID column returned as part of the query that can be used to identify and partition records, " +
                    "multiple columns can be entered in column-delimited order",
            required = false
    )
    private String identifierCol = null;

    @ConfigurationProperty(
            path = "idle_timeout",
            desc = "Amount of time in milliseconds to keep idle connections open. 0 for no limit",
            required = false
    )
    private int idleTimeout = 0;

    private JdbcIO.DataSourceConfiguration datasourceConfig;
    private ComboPooledDataSource initializationDS;
    private String viewName;
    private Schema schema;
    private String keyValueQuery;
    private Schema keyValueSchema;

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
     * <p>
     * By default, batch_size and identifier_col are optional but are highly recommended as the defaults may
     * not be optimal for performance
     *
     * @throws ComponentInitializationException if an error occurs during initialization or if configuraiton contains
     *                                          unexpected values
     */
    public void init() throws ComponentInitializationException {
        try {
            this.initializationDS = new ComboPooledDataSource();
            initializationDS.setAcquireRetryAttempts(1);
            initializationDS.setDriverClass(driver);
            initializationDS.setJdbcUrl(url);
            initializationDS.setUser(user);
            initializationDS.setPassword(password);
            initializationDS.setMaxIdleTime(this.idleTimeout);
            ComboPooledDataSource ds = new ComboPooledDataSource();
            ds.setDriverClass(driver);
            ds.setJdbcUrl(url);
            ds.setUser(user);
            ds.setPassword(password);
            ds.setMaxIdleTime(this.idleTimeout);
            this.datasourceConfig = JdbcIO.DataSourceConfiguration
                    .create(ds);
            // We will first preflight with a query that counts the number of records so that we can get number
            // of batches
            String runId = UUID.randomUUID().toString().replaceAll("-", "_");
            this.viewName = "backbone_jdbcextract_" + runId;
            if (this.identifierCol != null) {
                this.keyValueQuery = "SELECT DISTINCT " + identifierCol + " FROM (" + query + ") " + viewName;
                this.keyValueSchema = getIdentifierColumnsSchema();
            }
        } catch (Throwable t) {
            throw new ComponentInitializationException(t);
        }
    }

    @Override
    public List<String> getOutputTags() {
        return Collections.singletonList("JDBC Results");
    }

    @Override
    public Schema calculateOutputSchema() {
        Schema schema;
        try (Connection conn = initializationDS.getConnection();
             PreparedStatement ps = conn.prepareStatement(this.query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            schema = SchemaUtilProxy.toBeamSchema(driver, ps.getMetaData());
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
        this.schema = schema;
        return this.schema;
    }

    @Override
    public PCollection<Row> begin(PBegin input) {
        if (this.identifierCol == null) {
            return input.apply(
                    "Read from JDBC",
                    JdbcIO.<Row>read()
                    .withDataSourceConfiguration(datasourceConfig)
                    .withQuery("SELECT * FROM (" + this.query + ") " + this.viewName)
                    .withRowMapper(this.driver.equals("org.sqlite.JDBC") ?
                            new SchemaUtilProxy.SQLiteBeamRowMapperProxy(schema) :
                            new SchemaUtilProxy.BeamRowMapperProxy(schema))
                    .withCoder(RowCoder.of(schema))
                    .withOutputParallelization(false)
            ).apply("JDBC Break Fusion", Repartition.of()).setRowSchema(schema);
        } else {
            StringBuilder queryByKey = new StringBuilder("SELECT * FROM (" + this.query + ") " + this.viewName + " WHERE ");
            boolean appendAnd = false;
            for (String identifierCol : this.identifierCol.split(",")) {
                if (appendAnd) {
                    queryByKey.append("AND ");
                } else {
                    appendAnd = true;
                }
                queryByKey.append(identifierCol).append(" = ? ");
            }
            JdbcIO.RowMapper<Row> rowmapper = this.driver.equals("org.sqlite.JDBC") ?
                    new SchemaUtilProxy.SQLiteBeamRowMapperProxy(keyValueSchema) :
                    new SchemaUtilProxy.BeamRowMapperProxy(keyValueSchema);
            String[] cols = this.identifierCol.split(",");
            return input.apply("JDBC Init", Create.of(keyValueQuery))
                    .apply("JDBC Preflight for Query Keys", ParDo.of(
                            new DoFn<String, Row>() {
                                private ComboPooledDataSource ds;

                                @Setup
                                public void init() throws PropertyVetoException {
                                    this.ds = new ComboPooledDataSource(); // Set separate
                                    ds.setDriverClass(driver);
                                    ds.setJdbcUrl(url);
                                    ds.setUser(user);
                                    ds.setPassword(password);
                                    ds.setMaxIdleTime(idleTimeout);
                                }

                                @ProcessElement
                                public void process(ProcessContext pc) throws Exception {
                                    try (Connection conn = ds.getConnection()) {
                                        ResultSet rs = conn.createStatement().executeQuery(pc.element());
                                        while (rs.next()) {
                                            pc.output(rowmapper.mapRow(rs));
                                        }

                                    } catch (SQLException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            }
                    )).setRowSchema(this.keyValueSchema)
                    .apply("JDBC Break Fusion", Repartition.of()) // Break fusion here due to large fanout/preflight being on single thread
                    .apply("JDBC Read", JdbcIO.<Row, Row>readAll()
                            .withDataSourceConfiguration(datasourceConfig)
                            .withQuery(queryByKey.toString())
                            .withRowMapper(this.driver.equals("org.sqlite.JDBC") ?
                                    new SchemaUtilProxy.SQLiteBeamRowMapperProxy(schema) :
                                    new SchemaUtilProxy.BeamRowMapperProxy(schema))
                            .withParameterSetter((JdbcIO.PreparedStatementSetter<Row>) (element, preparedStatement) -> {
                                for (int i = 0; i < cols.length; i++) {
                                    String col = cols[i];
                                    if (col.startsWith("\"")) {
                                        col = col.substring(1, col.length() - 1);
                                    }
                                    preparedStatement.setObject(i + 1, element.getValue(col));
                                }
                            })
                            .withCoder(RowCoder.of(schema))
                            .withOutputParallelization(false));
        }
    }

    private Schema getIdentifierColumnsSchema() throws ComponentInitializationException {
        try (Connection conn = this.initializationDS.getConnection()) {
            String metaQuery = "SELECT " + this.identifierCol + " FROM (" + this.query + ") " + this.viewName;
            ResultSetMetaData queryMeta = conn.prepareStatement(metaQuery).getMetaData();
            return SchemaUtilProxy.toBeamSchema(this.driver, queryMeta);
        } catch (SQLException e) {
            throw new ComponentInitializationException(e);
        }
    }

    private String[] findPaginationOrderingColumns(String query) throws ComponentInitializationException {
        try (Connection conn = this.initializationDS.getConnection()) {
            ResultSetMetaData queryMeta = conn.prepareStatement(query).getMetaData();
            Map<String, Integer> colNameToIndex = new HashMap<>();
            for (int i = 0; i < queryMeta.getColumnCount(); i++) {
                // Assume we are using a case-sensitive impl:
                // config mismatches can be addressed via config change for identifierCol but same is not true if
                // impl is case-sensitive and we try to use a case-normalized column name via automatic selection
                colNameToIndex.put(queryMeta.getColumnLabel(i + 1), i + 1);
            }
            if (this.identifierCol != null) {
                // User-supplied identifier column exists, make sure it actually exists in query results
                String toCheck = this.identifierCol;
                if (this.identifierCol.startsWith("\"") && this.identifierCol.endsWith("\"")) {
                    toCheck = toCheck.substring(1, toCheck.length() - 1);
                }
                if (!colNameToIndex.containsKey(toCheck)) {
                    throw new ComponentInitializationException(
                            new IllegalArgumentException("The supplied identifier_col " + this.identifierCol + " " +
                                    "does not exist in the returned query results. Available columns: " +
                                    Arrays.toString(colNameToIndex.keySet().toArray(new String[0]))));
                }
                return new String[]{this.identifierCol};
            } else {
                // User did not supply an identifier column, so instead we will concatenate by column index so that
                // ordering is done for all columns...
                // Ideally we would want to instead look for a pk/unique constraint and order on that instead of on
                // everything
                List<String> colNames = new ArrayList<>();
                for (int i = 0; i < queryMeta.getColumnCount(); i++) {
                    colNames.add(queryMeta.getColumnLabel(i + 1));
                }
                return colNames.toArray(new String[0]);
            }
        } catch (SQLException t) {
            throw new ComponentInitializationException(t);
        }

    }
}
