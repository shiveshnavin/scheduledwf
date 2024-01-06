package io.github.jas34.scheduledwf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.mysql.dao.MySQLExecutionDAO;
import com.netflix.conductor.mysql.util.QueryFunction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySQLExecutionDAOExt extends MySQLExecutionDAO {
    DataSource ds;
    ObjectMapper om;

    public MySQLExecutionDAOExt(ObjectMapper objectMapper, DataSource dataSource) {
        super(objectMapper, dataSource);
        this.ds = dataSource;
        this.om = objectMapper;
    }


    ResultSet runQuery(String sqlQuery){
        ResultSet resultSet = null;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {

            resultSet = preparedStatement.executeQuery();

        } catch (SQLException e) {
            e.printStackTrace(); // Handle the exception based on your application's needs
        }
        return resultSet;
    }
}
