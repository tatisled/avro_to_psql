package com.github.tatisled.config;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;

public class ConnectionConfig {

    private static final String CLOUD_SQL_CONNECTION_NAME = System.getenv("CLOUD_SQL_CONNECTION_NAME");
    private static final String DB_NAME = System.getenv("DB_NAME");
    private static final String DB_USER = System.getenv("DB_USER");
    private static final String DB_PASS = System.getenv("DB_PASS");

    public static ComboPooledDataSource getConnectionConfig() throws PropertyVetoException {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setDriverClass("org.postgresql.Driver");
        dataSource.setJdbcUrl(String.format("jdbc:postgresql:///%s"
                        + "?cloudSqlInstance=%s"
                        + "&socketFactory=com.google.cloud.sql.postgres.SocketFactory"
                , DB_NAME
                , CLOUD_SQL_CONNECTION_NAME));
        dataSource.setUser(DB_USER);
        dataSource.setPassword(DB_PASS);
        dataSource.setMaxPoolSize(10);
        dataSource.setInitialPoolSize(6);

        return dataSource;
    }
}
