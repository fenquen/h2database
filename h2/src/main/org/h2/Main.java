package org.h2;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Main {
    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");

        Connection connection = DriverManager.getConnection("jdbc:h2:file:./data", "root", "root");
        Connection connectiona = DriverManager.getConnection("jdbc:h2:file:./data", "root", "root");

        connection.setAutoCommit(false);
        connectiona.setAutoCommit(false);

        connection.createStatement().execute("update TEST_TABLE1 set name1 = 'a'");
        connectiona.prepareStatement("select * from TEST_TABLE1").executeQuery();
        connection.commit();

        ResultSet resultSet = connectiona.prepareStatement("select * from TEST_TABLE1").executeQuery();
        resultSet.next();

        System.out.println(resultSet.getString("NAME1"));
    }
}
