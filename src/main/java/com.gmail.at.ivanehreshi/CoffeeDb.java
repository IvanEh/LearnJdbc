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
    // Without '' around %s doesn't work
    static final String SQL_INSERT_COFFEE = "INSERT INTO coffee(name) VALUES ( '%s' )";

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
            printException(e);
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
                printException(e);
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
            printException(e);
        }

    }

    public void batchInsertThreeTopCoffees(String top1, String top2, String top3) {
        Optional<Connection> maybeConn = getConnection();
        Connection connection;
        if(maybeConn.isPresent())
            connection = maybeConn.get();
        else
            return ;

        // To fully utilize batch updates auto commit should be false
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            printException(e);
        }

        try(Statement statement = connection.createStatement()) {
            statement.addBatch(String.format(SQL_INSERT_COFFEE, top1));
            statement.addBatch(String.format(SQL_INSERT_COFFEE, top2));
            statement.addBatch(String.format(SQL_INSERT_COFFEE, top3));
            int[] updateCount = statement.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            printException(e);
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


            System.out.printf("Holdability - hold: %s\n", metaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
            System.out.printf("Holdability - close: %s\n", metaData.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));

            System.out.printf("Concurrency - forward only - read only:  %s\n",
                    metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
            System.out.printf("Concurrency - forward only - updatable:  %s\n",
                    metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
            System.out.printf("Concurrency - scroll insensitive - updatable:  %s\n",
                    metaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE));
            System.out.printf("Concurrency - scroll sensitive - updatable:  %s\n",
                    metaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE));

        } catch (SQLException e) {
            printException(e);
        }

    }

    public static void printException(SQLException e) {
        if(e == null)
            return;

        System.out.println("SQL state(5 alphanums): " + e.getSQLState());
        System.out.println("Error code(vendor specific exception code): " + e.getErrorCode());
        System.out.println("Description: " + e.getMessage());

        SQLException nextException = e.getNextException();
        if(nextException != null) {
            System.out.println("|--->");
            printException(nextException);
        }
    }

    public static void printWarnings(SQLWarning warning) {
        if(warning == null) {
            System.out.println("No warnings");
            return;
        }

        System.out.println("Warnings: ");
        printException(warning);
        SQLWarning nextWarning = warning.getNextWarning();
        if(nextWarning != null) {
            System.out.println("|--->");
            printWarnings(nextWarning);
        }
    }

    public static void main(String[] args) {
        CoffeeDb db = new CoffeeDb();
        db.connect();
        db.dbInfo();
        db.batchInsertThreeTopCoffees("Tropicana", "Mogi", "Cheb");
        db.printAllCoffeeFromLast();
        db.close();
    }
}
