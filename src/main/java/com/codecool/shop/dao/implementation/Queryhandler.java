package com.codecool.shop.dao.implementation;

import com.codecool.shop.exception.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public interface Queryhandler {

    final class QueryLogger {
        private static final Logger logger = LoggerFactory.getLogger(Queryhandler.class);
    }

    String getConnectionConfigPath();

    default Connection getConnection() {
        QueryLogger.logger.info("Getting connection");
        Properties connection_props = new Properties();
        try {
            QueryLogger.logger.debug("Trying to read connection config file at {}", getConnectionConfigPath());
            connection_props.load(new FileInputStream(getConnectionConfigPath()));
        } catch (IOException e) {
            QueryLogger.logger.error("Cannot load connection properties", e);
            throw new DatabaseException("Can't connect to database");
            //System.out.println(e.getMessage());
            //e.printStackTrace(System.out);
        }
        String db_name = connection_props.getProperty("db_name");
        String db_url = connection_props.getProperty("db_url");
        String db_user = connection_props.getProperty("db_user");
        String db_password = connection_props.getProperty("db_password");
        String db_address = "jdbc:postgresql://" + db_url + "/" + db_name;

        Connection connection = null;
        try {
            QueryLogger.logger.debug("Trying to connect to database as user: {}", db_user);
            connection = DriverManager.getConnection(
                    db_address,
                    db_user,
                    db_password);
        } catch (SQLException e) {
            QueryLogger.logger.error("Cannot connect to databse", e);
            throw new DatabaseException("Can't connect to database");
            //System.out.println(e.getMessage());
            //e.printStackTrace(System.out);
        }
        QueryLogger.logger.info("Succesful connection, returning connection object");
        return connection;
    }

    default PreparedStatement createPreparedStatement(Connection connection, String query, List<Object> parameters)
            throws SQLException {
        PreparedStatement statement = connection.prepareStatement(query);

        Integer index = 1;
        for (Object parameter : parameters) {
            statement.setObject(index, parameter);
            index++;
        }
        return  statement;
    }

    default Integer executeDMLQuery(String query) {
        QueryLogger.logger.info("Trying to executed DML SQL query without parameters");
        Integer result = null;
        try (Connection connection = getConnection();
             PreparedStatement statement = connection.prepareStatement(query);
        ){
            QueryLogger.logger.debug("Executing DML SQL query without parameters: {}", statement.toString());
            result = statement.executeUpdate();
        } catch (SQLException e) {
            QueryLogger.logger.error("SQL query failed", e);
            throw new DatabaseException("Can't complete request");
        }
        QueryLogger.logger.info("Executed DML SQL query without parameters");
        return result;
    }

    default Integer executeDMLQuery(String query, List<Object> parameters) {
        QueryLogger.logger.info("Trying to executed DML SQL query with parameters");
        Integer result = null;
        try (Connection connection = getConnection();
             PreparedStatement statement = createPreparedStatement(connection, query, parameters);
        ){
            QueryLogger.logger.debug("Executing DML SQL query with parameters: {}", statement.toString());
            result = statement.executeUpdate();
        } catch (SQLException e) {
            QueryLogger.logger.error("SQL query failed", e);
            throw new DatabaseException("Can't complete request");

        }
        QueryLogger.logger.info("Executed DML SQL query with parameters");
        return result;
    }

    default List<Map<String, Object>> executeSelectQuery(String query) {
        QueryLogger.logger.info("Trying to executed Select SQL query without parameters");
        List<Map<String, Object>> resultListOfMaps = new ArrayList<>();
        try (Connection connection = getConnection();
             PreparedStatement statement = connection.prepareStatement(query)
        ){
            QueryLogger.logger.debug("Executing Select SQL query without parameters: {}", statement.toString());
            ResultSet resultSet = statement.executeQuery();
            ResultSetMetaData metadata = resultSet.getMetaData();
            int numberOfColumns = metadata.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> tempMap = new HashMap<>();
                for(int i=1; i<=numberOfColumns; i++){
                    tempMap.put(metadata.getColumnName(i),resultSet.getObject(i));
                }
                resultListOfMaps.add(tempMap);
            }
        } catch (SQLException e) {
            QueryLogger.logger.error("SQL query failed", e);
            throw new DatabaseException("Can't complete request");
        }
        QueryLogger.logger.info("Executed Select SQL query without parameters completed");
        return resultListOfMaps;
    }

    default List<Map<String, Object>> executeSelectQuery(String query, List<Object> parameters) {
        QueryLogger.logger.info("Trying to executed Select SQL query with parameters");
        QueryLogger.logger.info("Executing SQL Select query with parameters");
        List<Map<String, Object>> resultListOfMaps = new ArrayList<>();
        try (Connection connection = getConnection();
             PreparedStatement statement = createPreparedStatement(connection, query, parameters)
        ){
            QueryLogger.logger.debug("Executing Select SQL query with parameters: {}", statement.toString());
            ResultSet resultSet = statement.executeQuery();
            ResultSetMetaData metadata = resultSet.getMetaData();
            int numberOfColumns = metadata.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> tempMap = new HashMap<>();
                for(int i=1; i<=numberOfColumns; i++){
                    tempMap.put(metadata.getColumnName(i),resultSet.getObject(i));
                }
                resultListOfMaps.add(tempMap);
            }
        } catch (SQLException e) {
            QueryLogger.logger.error("SQL query failed", e);
            throw new DatabaseException("Can't complete request");
        }
        QueryLogger.logger.info("Executed Select SQL query with parameters completed");
        return resultListOfMaps;
    }
}
