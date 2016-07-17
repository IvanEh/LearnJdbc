package com.gmail.at.ivanehreshi;

import com.mysql.jdbc.*;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import com.sun.rowset.CachedRowSetImpl;
import com.sun.rowset.JdbcRowSetImpl;
import com.sun.rowset.RowSetFactoryImpl;

import javax.sql.RowSet;
import javax.sql.RowSetEvent;
import javax.sql.RowSetListener;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import javax.sql.rowset.spi.SyncProviderException;
import javax.sql.rowset.spi.SyncResolver;
import java.io.IOException;
import java.sql.*;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Optional;
import java.util.Random;

    // I'd suggest to use normal insert and update statements unless you really have to do something else.
    // I have yet to find a good use case for row sets myself, and I'd stay away unless you find a correct
    // implementation. Something which com.sun.rowset.CachedRowSetImpl really isn't; I have run into too
    // many different problems ranging from not following the JDBC standard with regard to the difference
    // between columnName and columnLabel, problems with using it in auto commit (or not auto commit;
    // the details are a bit fuzzy), to things like that difference between setString and updateString)
public class CoffeeDb {
    MysqlDataSource ds;
    String dbName = "coffee_shop";
    String user = "ivaneh";
    String password = "password";
    Connection connection = null;
    static final String SQL_GET_ALL_COFFEE = "SELECT id_coffee, name, price, image FROM coffee";

    public CoffeeDb() {
        this.ds = new MysqlDataSource();
        ds.setDatabaseName(dbName);
        ds.setUser(user);
        ds.setPassword(password);
        ds.setServerName("localhost");
        ds.setPort(3306);
        try {
            connection = ds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createRowsetAndDoStuff() {
        try(Statement stmt = this.connection.createStatement(
                ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE)) {
            ResultSet rs = stmt.executeQuery(SQL_GET_ALL_COFFEE);

            doStuff(rs);

            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

    private void doStuff(ResultSet rs) throws SQLException {
        while (rs.next()) {
            System.out.print(rs.getString("name"));
            System.out.println(" " + rs.getDouble("price"));

            Blob blob = this.connection.createBlob();
            try {
//                for some reasons this doesn't works with mysql connector
//                blob.setBinaryStream(1).write(new byte[]{1, 1, 0, 1});
                blob.setBytes(1, new byte[]{1, 1, 0, 1});
                System.out.println(blob.length());
                rs.updateBlob("image", blob);
            } catch (Exception e) {
                e.printStackTrace();
            }
            rs.updateRow();

            Blob inputBlob = rs.getBlob("image");
            System.out.println(inputBlob.getBytes(1, 4)[1]);
            inputBlob.free();
        }
    }


    public static void main(String[] args) {
        CoffeeDb db = new CoffeeDb();
        db.createRowsetAndDoStuff();
        db.close();
    }

    private void close() {
        if(this.connection != null)
            try {
                this.connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }
}
