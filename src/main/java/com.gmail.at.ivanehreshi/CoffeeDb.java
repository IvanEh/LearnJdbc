package com.gmail.at.ivanehreshi;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import java.sql.*;
import java.util.Optional;

public class CoffeeDb {
    MysqlDataSource ds;
    String dbName = "coffee_shop";
    String user = "ivaneh";
    String password = "password";
    Connection connection = null;
    static final String SQL_GET_ALL_COFFEE_ALPHA_ORD = "SELECT name FROM coffee ORDER BY name ASC";
    static final String SQL_INSERT_COFFEE = "INSERT INTO coffee(name, price) VALUES (?, ?)";

    public CoffeeDb() {
        this.ds = new MysqlDataSource();
        ds.setDatabaseName(dbName);
        ds.setUser(user);
        ds.setPassword(password);
        ds.setServerName("localhost");
        ds.setPort(3306);
    }

    public void connect() {
        /**
         * DataSource is the preffered method because it is more flexible in the
         * terms of property changing. It supports connection pools(@ref ConnectionPoolDataSource_
         */
        try {
            connection = ds.getConnection();
        } catch (SQLException e){
            e.printStackTrace();
            connection = null;
        }
    }

    public Optional<Connection> getConnection() {
        return Optional.ofNullable(connection);
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void close() {
        getConnection().ifPresent(conn -> {
            try { conn.close(); }
            catch (SQLException e) {
                e.printStackTrace();
            }
        });
        setConnection(null);
    }

    public void printAllCoffeeFromLast() {
        Optional<Connection> maybeConn = getConnection();
        Connection connection;
        if(maybeConn.isPresent())
            connection = maybeConn.get();
        else
            return ;

        try(Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(SQL_GET_ALL_COFFEE_ALPHA_ORD);
            rs.afterLast();

            while (rs.previous()) {
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void dbInfo() {
        Optional<Connection> maybeConnection = getConnection();
        Connection connection;
        if(maybeConnection.isPresent()) {
            connection = maybeConnection.get();
        } else {
            return;
        }
        try {
            DatabaseMetaData metaData = connection.getMetaData();

            System.out.printf("Support type TYPE_FORWARD_ONLY: %s \n", metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
            System.out.printf("Support type TYPE_SCROLL_INSENSITIVE:  %s\n", metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
            System.out.printf("Support type TYPE_SCROLL_SENSITIVE: %s \n", metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));

            System.out.println();
            System.out.printf("Holdability - hold: %s\n", metaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
            System.out.printf("Holdability - close: %s\n", metaData.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));

            System.out.println();
            System.out.printf("Concurrency - forward only - read only:  %s\n",
                    metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
            System.out.printf("Concurrency - forward only - updatable:  %s\n",
                    metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
            System.out.printf("Concurrency - scroll insensitive - updatable:  %s\n",
                    metaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE));
            System.out.printf("Concurrency - scroll sensitive - updatable:  %s\n",
                    metaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE));

            System.out.println();
            System.out.printf("Transaction isolation level - read committed: %s\n",
                    metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
            System.out.printf("Transaction isolation level - read uncommitted: %s\n",
                    metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
            System.out.printf("Transaction isolation level - repeatable read: %s\n",
                    metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
            System.out.printf("Transaction isolation level - serializable read: %s\n",
                    metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    public static void main(String[] args) {
        CoffeeDb db = new CoffeeDb();
        db.connect();
        db.dbInfo();
        db.close();
    }
}
