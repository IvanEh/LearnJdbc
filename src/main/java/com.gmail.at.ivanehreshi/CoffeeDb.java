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
    static final String SQL_DOMAIN = "SELECT magic_column FROM coffee";

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

    public void domainMethod() {
        Optional<Connection> maybeConn = getConnection();
        Connection connection;
        if(maybeConn.isPresent())
            connection = maybeConn.get();
        else
            return;


        try(Statement statement = connection.createStatement()) {
            DatabaseMetaData metaData = connection.getMetaData();

            ResultSet rs = statement.executeQuery(SQL_DOMAIN);
            while (rs.next()) {
                System.out.println(rs.getString("magic_column"));
            }
            printWarnings(statement.getWarnings());
            printWarnings(rs.getWarnings());
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
        db.domainMethod();
        db.close();
    }
}
