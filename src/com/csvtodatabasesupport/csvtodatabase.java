package com.csvtodatabasesupport;

import static java.lang.Integer.parseInt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class csvtodatabase {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String jdbcUrl = "jdbc:mysql://localhost:3306/greekforgreekproject";
		String username = "root";
		String password = "master#123";

		String filePath = "D:\\Downloads\\data.csv";
		int batchSize = 20;

		Connection connection = null;
		try {
			 connection= DriverManager.getConnection(jdbcUrl,username,password);
            connection.setAutoCommit(false);

            String sql="insert into itemslist(id,name,quantity,cost)values(?,?,?,?)";
            String updateSql = "UPDATE itemslist SET name = ?, quantity = ?, cost = ? WHERE id = ?";


            PreparedStatement statement=connection.prepareStatement(sql);
            
            PreparedStatement updatestatement=connection.prepareStatement(updateSql);
            
            

            BufferedReader lineReader=new BufferedReader(new FileReader(filePath));

            String lineText=null;
            int count=0;

            lineReader.readLine();
            while ((lineText=lineReader.readLine())!=null){
                String[] data =lineText.split(",");
                String id = data[0];
                String name = data[1];
                String quantity = data[2];
                String cost = data[3];
                
                if (recordExists(connection, parseInt(id))) {
                    // Prepare an update operation
                    updatestatement.setString(1, name);
                    updatestatement.setString(2, quantity);
                    updatestatement.setString(3, cost);
                    updatestatement.setInt(4, parseInt(id));
                    updatestatement.addBatch();
                } else {
                    // Prepare an insert operation
                    statement.setInt(1, parseInt(id));
                    statement.setString(2, name);
                    statement.setString(3, quantity);
                    statement.setString(4, cost);
                    statement.addBatch();
                }

                if(count % batchSize == 0){
                    statement.executeBatch();
                    updatestatement.executeBatch();
                }
            }
            lineReader.close();
            
            statement.executeBatch();
            updatestatement.executeBatch();
            connection.commit();
            connection.close();

            System.out.println("Data has been inserted successfully");

		} catch (Exception exception) {
			exception.printStackTrace();
		}

	}
	private static boolean recordExists(Connection connection, int id) throws Exception {
        String selectSql = "SELECT id FROM itemslist WHERE id = ?";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        selectStatement.setInt(1, id);
        ResultSet resultSet = selectStatement.executeQuery();
        return resultSet.next();
    }
}
