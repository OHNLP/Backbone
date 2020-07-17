package org.ohnlp.backbone.io.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Extract;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;

/**
 * Performs data extraction using a JDBC connector
 * This class is essentially a configuration wrapper around the beam provided {@link JdbcIO} transform
 */
public class JDBCExtract extends Extract {
    private JdbcIO.ReadRows runnableInstance;

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
            String query = config.get("query").asText();
            JdbcIO.DataSourceConfiguration datasourceConfig = JdbcIO.DataSourceConfiguration
                    .create(driver, url)
                    .withUsername(user)
                    .withPassword(password);
            runnableInstance = JdbcIO.readRows()
                    .withDataSourceConfiguration(datasourceConfig)
                    .withQuery(query);
        } catch (Throwable t) {
            throw new ComponentInitializationException(t);
        }
    }


    public PCollection<Row> expand(PBegin input) {
        return runnableInstance.expand(input);
    }
}
