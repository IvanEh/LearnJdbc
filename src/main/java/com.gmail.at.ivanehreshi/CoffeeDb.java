package com.gmail.at.ivanehreshi;

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
import java.sql.*;
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
    static final String SQL_GET_ALL_COFFEE = "SELECT id_coffee, name, price FROM coffee";

    public CoffeeDb() {
        this.ds = new MysqlDataSource();
        ds.setDatabaseName(dbName);
        ds.setUser(user);
        ds.setPassword(password);
        ds.setServerName("localhost");
        ds.setPort(3306);
    }

    public void createRowsetAndDoStuff() {
        CachedRowSet rowSet = null;
        try {
            rowSet = new CachedRowSetImpl();
            // Note that relaxAutoCommit variable need to be set
            rowSet.setUrl("jdbc:mysql://" + "localhost:3306/" + dbName + "?relaxAutoCommit=true");
            rowSet.setUsername(user);
            rowSet.setPassword(password);
            rowSet.setCommand(SQL_GET_ALL_COFFEE);
            rowSet.execute();

            // cached rowset can be paged. See documentation
            System.out.println("Page size: " + rowSet.getPageSize());

            rowSet.addRowSetListener(new RowSetListener() {
                @Override
                public void rowSetChanged(RowSetEvent event) {
                    System.out.println("RowSetEvent");
                }

                @Override
                public void rowChanged(RowSetEvent event) {

                }

                @Override
                public void cursorMoved(RowSetEvent event) {

                }
            });

            doStuff(rowSet);

            // update database
            rowSet.acceptChanges();

        } catch (SyncProviderException e) {
            SyncResolver resolver = e.getSyncResolver();
//            resolver.nextConflict();
//            resolver.getConflictValue(index | column)

        }
        catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                rowSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void doStuff(CachedRowSet rowSet) throws SQLException {
        while (rowSet.next()) {
            System.out.print(rowSet.getString("name"));
            System.out.println(" " + rowSet.getDouble("price"));

            if(new Random().nextBoolean())
                continue;

            rowSet.updateDouble("price", rowSet.getDouble("price") * 2);
            rowSet.updateRow();
        }
//  insertion doesn't works

//        rowSet.moveToInsertRow();
//        rowSet.updateString("name", "NotFiltered");
//        rowSet.updateDouble("price", 15);
//        rowSet.updateNull("id_coffee");
//        rowSet.insertRow();
    }


    public static void main(String[] args) {
        CoffeeDb db = new CoffeeDb();
        db.createRowsetAndDoStuff();
    }
}
