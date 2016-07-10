package com.gmail.at.ivanehreshi;

import javax.rmi.CORBA.Util;
import java.sql.*;
import java.util.Optional;
import java.util.concurrent.Exchanger;

public class Db {
    String url = "jdbc:mysql://localhost:3306/sample_schema";
    String user = "ivaneh";
    String password = "password";
    Connection connection = null;
    final String SQL_DOMAIN = "SELECT magic_column FROM sample_table";

    public void connect() {
        try {
            connection = DriverManager.getConnection(url, user, password);
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
