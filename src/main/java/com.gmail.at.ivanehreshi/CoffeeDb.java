package com.gmail.at.ivanehreshi;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import com.sun.rowset.JdbcRowSetImpl;
import com.sun.rowset.RowSetFactoryImpl;

import javax.sql.RowSet;
import javax.sql.rowset.RowSetProvider;
import java.sql.*;
import java.util.Optional;
import java.util.Random;

public class CoffeeDb {
    MysqlDataSource ds;
    String dbName = "coffee_shop";
    String user = "ivaneh";
    String password = "password";
    Connection connection = null;
    static final String SQL_GET_ALL_COFFEE = "SELECT id_coffee, name, price FROM coffee";

    public CoffeeDb() {
        this.ds = new MysqlDataSource();
        ds.setDatabaseName(dbName);
        ds.setUser(user);
        ds.setPassword(password);
        ds.setServerName("localhost");
        ds.setPort(3306);
    }

    public void rowsetFromConnection() {
        RowSet rowSet = null;
        try {
            rowSet = new JdbcRowSetImpl(ds.getConnection());
            rowSet.setCommand(SQL_GET_ALL_COFFEE);
            rowSet.execute();
            doStuff(rowSet);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                rowSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void rowsetFromRs() {
        RowSet rowSet = null;
        try {
            // resultset should be updatable and scrollable in order to use rowset
            Statement stmt = ds.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                                ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = stmt.executeQuery(SQL_GET_ALL_COFFEE);
            rowSet = new JdbcRowSetImpl(rs);
            doStuff(rowSet);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                rowSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void rowsetFromDefaultCtor() {
        RowSet rowSet = null;
        try {
            // resultset should be updatable and scrollable in order to use rowset
            rowSet = new JdbcRowSetImpl();
            rowSet.setUrl("jdbc:mysql://" + "localhost:3306/" + dbName);
            rowSet.setUsername(user);
            rowSet.setPassword(password);
            rowSet.setCommand(SQL_GET_ALL_COFFEE);
            rowSet.execute();
            doStuff(rowSet);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                rowSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void rowsetFromFactory() {
        RowSet rowSet = null;
        try {
            // Works
            //rowSet = new RowSetFactoryImpl().createJdbcRowSet();
            // but preferred
            rowSet = RowSetProvider.newFactory().createJdbcRowSet();
            rowSet.setUrl("jdbc:mysql://" + "localhost:3306/" + dbName);
            rowSet.setUsername(user);
            rowSet.setPassword(password);
            rowSet.setCommand(SQL_GET_ALL_COFFEE);
            rowSet.execute();
            doStuff(rowSet);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                rowSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void doStuff(RowSet rowSet) throws SQLException {
        System.out.println("------------");
        System.out.println("query");
        while (rowSet.next()) {
            System.out.printf("%s(%s)\n", rowSet.getString("name"),
                                        rowSet.getString("price"));
        }
        rowSet.beforeFirst();
        if(rowSet.next()) {
            System.out.printf("%s(%s)\n", rowSet.getString("name"),
                    rowSet.getString("price"));
        }

        System.out.println("update");
        rowSet.afterLast();
        if(rowSet.previous()) {
            rowSet.updateDouble("price", new Random().nextDouble() * 20);
        }
        rowSet.updateRow();

        System.out.println("insert");
        rowSet.moveToInsertRow();
        rowSet.updateString("name", "Random" + new Random().nextInt(7));
        rowSet.insertRow();
        System.out.println("------------");
    }


    public static void main(String[] args) {
        CoffeeDb db = new CoffeeDb();
        db.rowsetFromConnection();
        System.out.println();
        db.rowsetFromRs();
        System.out.println();
        db.rowsetFromDefaultCtor();
        System.out.println();
        db.rowsetFromFactory();
    }
}
