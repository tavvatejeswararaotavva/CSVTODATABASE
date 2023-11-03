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
			connection = DriverManager.getConnection(jdbcUrl, username, password);
			connection.setAutoCommit(false);

			String sql = "insert into itemlist(id,name,quantity,cost)values(?,?,?,?)";

			PreparedStatement statement = connection.prepareStatement(sql);

			BufferedReader lineReader = new BufferedReader(new FileReader(filePath));

			String lineText = null;
			int count = 0;

			lineReader.readLine();
			while ((lineText = lineReader.readLine()) != null) {
				String[] data = lineText.split(",");

				String id = data[0];
				String name = data[1];
				String quantity = data[2];
				String cost = data[3];

				statement.setInt(1, parseInt(id));
				statement.setString(2, name);
				statement.setInt(3, parseInt(quantity));
				statement.setInt(4, parseInt(cost));
				statement.addBatch();

				if (count % batchSize == 0) {
					statement.executeBatch();
				}
			}
			lineReader.close();

			statement.executeBatch();
			connection.commit();
			connection.close();

			System.out.println("Data has been inserted successfully");

		} catch (Exception exception) {
			exception.printStackTrace();
		}

	}
}
