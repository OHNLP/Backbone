package org.ohnlp.backbone.io.databricks;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.ReadableDateTime;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.annotations.InputColumnProperty;
import org.ohnlp.backbone.api.components.LoadFromOne;
import org.ohnlp.backbone.api.config.InputColumn;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;

/**
 * Performs data load using a JDBC connector
 * This class is essentially a configuration wrapper around the beam provided {@link JdbcIO} transform with additional
 * support added for user-customizeable PreparedStatement usage
 *
 * <p>
 * Expected configuration structure:
 * <pre>
 *     {
 *         "url": "jdbc_url_to_database",
 *         "driver": "jdbc.driver.class",
 *         "user": "dbUsername",
 *         "password": "dbPassword",
 *         "query": "query_to_execute_for_load in preparedstatemnt format. Substitute ? for variables (1-indexed)",
 *         "paramMappings": [
 *              "this_column_name_will_be_mapped_to_the_first_?",
 *              "this_column_name_will_be_mapped_to_the_second_?",
 *              ...
 *              etc.
 *         ]
 *     }
 * </pre>
 */
@ComponentDescription(
        name = "Write Records into JDBC-Compatible Data Source",
        desc = "Writes records into a JDBC-compatible data source using a SQL query. The insert statements should " +
                "follow Java PreparedStatement format (e.g. use ? for parameterized variables). The columns in " +
                "the paramMappings configuration argument will be used to substitute values into the respective ? " +
                "in the insert query in sequential order."
)
public class DatabricksLoad extends LoadFromOne {
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
            desc = "Database Query to use as inserts. Use ? to indicate column value arguments. " +
                    "E.g., INSERT INTO tablename (column1, column2, column3) VALUES (?, ?, ?)"
    )
    private String query;
    @ConfigurationProperty(
            path = "paramMappings",
            desc = "List of columns to use as values to insert. Should follow same order as the ?s used in the insert query"
    )
    @InputColumnProperty
    private List<InputColumn> columnMappings;
    @ConfigurationProperty(
            path = "idleTimeout",
            desc = "Amount of time in milliseconds to keep idle connections open. 0 for no limit",
            required = false
    )
    private int idleTimeout = 0;
    private JdbcIO.Write<Row> runnableInstance;

    public void init() throws ComponentInitializationException {
        try {
            List<RowToPSMappingFunction> mappingOps = new LinkedList<>();
            int i = 1;
            for (InputColumn column : columnMappings) {
                mappingOps.add(new RowToPSMappingFunction(i++, column.getSourceColumnName()));
            }
            ComboPooledDataSource ds = new ComboPooledDataSource();
            ds.setDriverClass(driver);
            ds.setJdbcUrl(url);
            ds.setUser(user);
            ds.setPassword(password);
            ds.setMaxIdleTime(idleTimeout);
            JdbcIO.DataSourceConfiguration datasourceConfig = JdbcIO.DataSourceConfiguration.create(ds);
            this.runnableInstance = JdbcIO.<Row>write()
                    .withDataSourceConfiguration(datasourceConfig)
                    .withStatement(query)
                    .withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<Row>) (element, preparedStatement) -> {
                        for (RowToPSMappingFunction func : mappingOps) {
                            func.map(element, preparedStatement);
                        }
                    });
        } catch (Throwable t) {
            throw new ComponentInitializationException(t);
        }
    }

    public PDone expand(PCollection<Row> input) {
        return runnableInstance.expand(input);
    }

    // Handles execution of row to preparedstatement type mappings
    private static class RowToPSMappingFunction implements Serializable {
        private final String fieldName;
        private final int idx;
        private transient RowMappingFunction execute;

        public RowToPSMappingFunction(int index, String fieldName) {
            this.idx = index;
            this.fieldName = fieldName;
        }

        public void map(Row row, PreparedStatement ps) throws SQLException {
            // Do type resolution only once
            if (this.execute == null) {
                Schema.TypeName type = row.getSchema().getField(fieldName).getType().getTypeName();
                if (type.isLogicalType()) {
                    type = row.getSchema().getField(fieldName).getType().getLogicalType().getBaseType().getTypeName();
                }
                switch (type) {
                    case BYTE:
                        this.execute = (data, statement) -> {
                            @Nullable @UnknownKeyFor @Initialized Byte val = data.getByte(this.fieldName);
                            if (val != null) {
                                statement.setByte(this.idx, val);
                            } else {
                                statement.setNull(this.idx, Types.TINYINT);
                            }
                        };
                        break;
                    case INT16:
                        this.execute = (data, statement) -> {
                            Short val = data.getInt16(this.fieldName);
                            if (val != null) {
                                statement.setShort(this.idx, val);
                            } else {
                                statement.setNull(this.idx, Types.SMALLINT);
                            }
                        };
                        break;
                    case INT32:
                        this.execute = (data, statement) -> {
                            Integer val = data.getInt32(this.fieldName);
                            if (val != null) {
                                statement.setInt(this.idx, val);
                            } else {
                                statement.setNull(this.idx, Types.INTEGER);
                            }
                        };
                        break;
                    case INT64:
                        this.execute = (data, statement) -> {
                            Long val = data.getInt64(this.fieldName);
                            if (val != null) {
                                statement.setLong(this.idx, val);
                            } else {
                                statement.setNull(this.idx, Types.BIGINT);
                            }
                        };
                        break;
                    case DECIMAL:
                        this.execute = (data, statement) -> {
                            BigDecimal val = data.getDecimal(this.fieldName);
                            if (val != null) {
                                statement.setBigDecimal(this.idx, val);
                            } else {
                                statement.setNull(this.idx, Types.DECIMAL);
                            }
                        };
                        break;
                    case FLOAT:
                        this.execute = (data, statement) -> {
                            Float val = data.getFloat(this.fieldName);
                            if (val != null) {
                                statement.setFloat(this.idx, val);
                            } else {
                                statement.setNull(this.idx, Types.FLOAT);
                            }
                        };
                        break;
                    case DOUBLE:
                        this.execute = (data, statement) -> {
                            Double val = data.getDouble(this.fieldName);
                            if (val != null) {
                                statement.setDouble(this.idx, val);
                            } else {
                                statement.setNull(this.idx, Types.DOUBLE);
                            }
                        };
                        break;
                    case STRING:
                        this.execute = (data, statement) -> {
                            String val = data.getString(this.fieldName);
                            if (val != null) {
                                statement.setString(this.idx, val);
                            } else {
                                statement.setNull(this.idx, Types.VARCHAR);
                            }
                        };
                        break;
                    case DATETIME:
                        this.execute = (data, statement) -> {
                            ReadableDateTime val = data.getDateTime(this.fieldName);
                            if (val != null) {
                                statement.setTimestamp(this.idx, new Timestamp(val.getMillis()));
                            } else {
                                statement.setNull(this.idx, Types.TIMESTAMP);
                            }
                        };
                        break;
                    case BOOLEAN:
                        this.execute = (data, statement) -> {
                            Boolean val = data.getBoolean(this.fieldName);
                            if (val != null) {
                                statement.setBoolean(this.idx, val);
                            } else {
                                statement.setNull(this.idx, Types.BOOLEAN);
                            }
                        };
                        break;
                    case BYTES:
                        this.execute = (data, statement) -> {
                            byte[] val = data.getBytes(this.fieldName);
                            if (val != null) {
                                statement.setBytes(this.idx, val);
                            } else {
                                statement.setNull(this.idx, Types.VARBINARY);
                            }
                        };
                        break;
                    default:
                        // TODO revisit the other options available
                        throw new UnsupportedOperationException(type.name() + " is not currently a supported type for JDBC encoding");
                }
            }
            this.execute.run(row, ps);
        }
    }

    /**
     * Interface definition for a row mapping function
     */
    private interface RowMappingFunction {
        void run(Row row, PreparedStatement ps) throws SQLException;
    }
}
