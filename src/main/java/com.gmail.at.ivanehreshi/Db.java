package com.gmail.at.ivanehreshi;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

public class Db {
    MysqlDataSource ds;
    String dbName = "sample_schema";
    String user = "ivaneh";
    String password = "password";
    Connection connection = null;
    final String SQL_DOMAIN = "SELECT magic_column FROM sample_table";

    public Db() {
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
            catch (Exception e) {
                e.printStackTrace();
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
            ResultSet rs = statement.executeQuery(SQL_DOMAIN);
            while (rs.next()) {
                System.out.println(rs.getString("magic_column"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Db db = new Db();
        db.connect();
        db.domainMethod();
        db.close();
    }
}
