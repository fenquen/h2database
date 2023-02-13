package org.h2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Main {
    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection connection = DriverManager.getConnection("jdbc:h2:file:./data", "root", "root");
        PreparedStatement preparedStatement = connection.prepareStatement("select * from  TEST_TABLE");
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();


    }
}
