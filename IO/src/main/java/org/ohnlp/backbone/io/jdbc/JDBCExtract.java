package org.ohnlp.backbone.io.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.SchemaUtilProxy;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.*;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.sql.*;
import java.util.*;

/**
 * Performs data extraction using a JDBC connector
 * This class uses {@link JdbcIO} included with Apache Beam to execute parallelized retrieval queries
 * using offsets for pagination.
 */
@ComponentDescription(
        name = "Read Records from a JDBC-compatible data source",
        desc = "Reads Records from a JDBC-compatible data source using a SQL query. Any queries should ideally " +
                "include an indexed identifier column that can be used to rapidly paginate/partition results for " +
                "parallelized processing"
)
public class JDBCExtract extends Extract {
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
            desc = "An ID column returned as part of the query that can be used to identify and partition records.",
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
    private long numBatches;
    private ComboPooledDataSource ds;
    private String[] orderByCols;
    private String viewName;
    private String orderedQuery;
    private Schema schema;

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
            this.ds = new ComboPooledDataSource();
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
                this.numBatches = Math.round(Math.ceil((double) resultCount / this.batchSize));
            }
            // Normally I would say use Strings.join for the below, but this was causing cross-jvm issues
            // so we use the more portable stringbuilder instead...
            StringBuilder sB = new StringBuilder();
            boolean flag = false;
            for (String s : this.orderByCols) {
                if (flag) {
                    sB.append(", ");
                }
                sB.append(s);
                flag = true;
            }
            this.orderedQuery = "SELECT * FROM (" + this.query + ") " + this.viewName
                    + " ORDER BY " + sB.toString() + " ";
            // Now we have to add the offset/fetch in the dialect local format..
            // Specifically, postgres and MySQL are special in that they do not conform to the
            // SQL:2011 standard syntax
            if (driver.equals("org.postgresql.Driver") || driver.equals("com.mysql.jdbc.Driver")
                    || driver.equals("com.mysql.cj.jdbc.Driver") || driver.equals("org.sqlite.JDBC")) {
                this.orderedQuery += "LIMIT " + batchSize + " OFFSET ?";
            } else { // This is the SQL:2011 standard definition of an offset...fetch syntax
                this.orderedQuery += "OFFSET ? ROWS FETCH NEXT " + batchSize + " ROWS ONLY";
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
    public Map<String, Schema> calculateOutputSchema(Map<String, Schema> input) {
        Schema schema;
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(this.query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            schema = SchemaUtilProxy.toBeamSchema(driver, ps.getMetaData());
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
        this.schema = schema;
        return Collections.singletonMap(getOutputTags().get(0), this.schema);
    }

    private String[] findPaginationOrderingColumns(String query) throws ComponentInitializationException {
        try (Connection conn = this.ds.getConnection()) {
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
                if (!colNameToIndex.containsKey(this.identifierCol)) {
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

    public PCollectionRowTuple expand(PBegin input) {
        List<Integer> offsets = new ArrayList<>();
        for (int i = 0; i < numBatches; i++) {
            offsets.add(i * batchSize); // Create a sequence of batches at the appropriate offset
        }
        return PCollectionRowTuple.of(
                getOutputTags().get(0),
                input.apply("JDBC Preflight", Create.of(offsets)) // First create partitions # = to num batches
                        .apply("JDBC Read", // Now actually do the read, the readall function will execute one query per input partition
                                JdbcIO.<Integer, Row>readAll()
                                        .withDataSourceConfiguration(datasourceConfig)
                                        .withQuery(this.orderedQuery)
                                        .withRowMapper(this.driver.equals("org.sqlite.JDBC") ?
                                                new SchemaUtilProxy.SQLiteBeamRowMapperProxy(schema) :
                                                new SchemaUtilProxy.BeamRowMapperProxy(schema))
                                        .withParameterSetter((JdbcIO.PreparedStatementSetter<Integer>) (element, preparedStatement) -> {
                                            preparedStatement.setInt(1, element); // Replace
                                        })
                                        .withCoder(RowCoder.of(schema))
                                        .withOutputParallelization(false)
                        ));
    }
}
