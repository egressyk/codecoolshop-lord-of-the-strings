package com.codecool.shop.dao.implementation;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;

/**
 * The Queryhandler interface handles database connection and SQL queries, while preventing SQL injections.
 *
 * @author Kristof Egressy
 * @version 1.0
 * @since 2018-05-11
 */
public interface Queryhandler {

    /**
     * This method is used in the class for getting the file containing db connection credentials.
     * It has to be implemented by the implementing class.
     *
     * @return String file path
     */
    String getConnectionConfigPath();

    /**
     * Reading connection credentials from a file.
     * <p>
     * Uses {@link #getConnectionConfigPath()} to get the file path. It's looking for the following key-value pairs in it:
     * <ul>
     * <li>db_name=[name of the database]</li>
     * <li>db_url=[IP and port for the database eg.: "localhost:5432"]</li>
     * <li>db_user=[user name for logging into the database]</li>
     * <li>db_password=[password for the user name]</li>
     * </ul>
     *
     * @return opened connection instance
     */
    default Connection getConnection() {
        Properties connection_props = new Properties();
        try {
            connection_props.load(new FileInputStream(getConnectionConfigPath()));
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace(System.out);
        }
        String db_name = connection_props.getProperty("db_name");
        String db_url = connection_props.getProperty("db_url");
        String db_user = connection_props.getProperty("db_user");
        String db_password = connection_props.getProperty("db_password");
        String db_address = "jdbc:postgresql://" + db_url + "/" + db_name;

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(
                    db_address,
                    db_user,
                    db_password);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            e.printStackTrace(System.out);
        }
        return connection;
    }

    /**
     * Merges the SQL statement and the parameters into a query-ready prepared statement.
     *
     * @param connection a connection instance (can get one with {@link #getConnection()})
     * @param query      the SQL statement as a string with "?" placed in as placeholders for parameters
     * @param parameters the list of parameters to be merged into the statement (in the order of appearance in the query)
     * @return a {@link java.sql.PreparedStatement PreparedStatement object}
     * @throws SQLException if error occures in connection
     */
    default PreparedStatement createPreparedStatement(Connection connection, String query, List<Object> parameters)
            throws SQLException {
        PreparedStatement statement = connection.prepareStatement(query);

        Integer index = 1;
        for (Object parameter : parameters) {
            statement.setObject(index, parameter);
            index++;
        }
        return statement;
    }

    /**
     * Executes a DELETE, INSERT or UPDATE sql query
     *
     * @param query the query string
     * @return the number of rows affected in the process
     */
    default Integer executeDMLQuery(String query) {
        Integer result = null;
        try (Connection connection = getConnection();
             PreparedStatement statement = connection.prepareStatement(query);
        ) {
            result = statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Executes a DELETE, INSERT or UPDATE sql query
     *
     * @param query the query string (including "?" as placeholders for parameters)
     * @param parameters parameters for the query (the same orders as they appear in the query)
     * @return the number of rows affected in the process
     */
    default Integer executeDMLQuery(String query, List<Object> parameters) {
        Integer result = null;
        try (Connection connection = getConnection();
             PreparedStatement statement = createPreparedStatement(connection, query, parameters);
        ) {
            result = statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * Executes a SELECT sql query
     *
     * @param query the query string
     * @return a List of {@code Map<String, Object>} as the result of the query
     */
    default List<Map<String, Object>> executeSelectQuery(String query) {
        List<Map<String, Object>> resultListOfMaps = new ArrayList<>();
        try (Connection connection = getConnection();
             PreparedStatement statement = connection.prepareStatement(query)
        ) {
            ResultSet resultSet = statement.executeQuery();
            ResultSetMetaData metadata = resultSet.getMetaData();
            int numberOfColumns = metadata.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> tempMap = new HashMap<>();
                for (int i = 1; i <= numberOfColumns; i++) {
                    tempMap.put(metadata.getColumnName(i), resultSet.getObject(i));
                }
                resultListOfMaps.add(tempMap);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultListOfMaps;
    }

    /**
     * Executes a SELECT sql query
     *
     * @param query the query string (including "?" as placeholders for parameters)
     * @param parameters parameters for the query (the same orders as they appear in the query)
     * @return a List of {@code Map<String, Object>} as the result of the query
     */
    default List<Map<String, Object>> executeSelectQuery(String query, List<Object> parameters) {
        List<Map<String, Object>> resultListOfMaps = new ArrayList<>();
        try (Connection connection = getConnection();
             PreparedStatement statement = createPreparedStatement(connection, query, parameters)
        ) {
            ResultSet resultSet = statement.executeQuery();
            ResultSetMetaData metadata = resultSet.getMetaData();
            int numberOfColumns = metadata.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> tempMap = new HashMap<>();
                for (int i = 1; i <= numberOfColumns; i++) {
                    tempMap.put(metadata.getColumnName(i), resultSet.getObject(i));
                }
                resultListOfMaps.add(tempMap);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultListOfMaps;
    }
}
