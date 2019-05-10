package com.denniso.javajdbcdemo;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.*;
import java.sql.Timestamp;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

//https://vkuzel.com/spring-boot-jpa-hibernate-atomikos-postgresql-exception
@SpringBootApplication
public class JavaJdbcDemoApplication implements CommandLineRunner {

	@Autowired
	JdbcTemplate jdbcTemplate;

	private static final Logger log = LoggerFactory.getLogger(JavaJdbcDemoApplication.class);

	String url ="jdbc:postgresql://localhost:5432/dvdrental2";
	String username ="postgres";
	String password = "admin";

	public static void main(String[] args) {
		System.out.println("Hello World");
		SpringApplication.run(JavaJdbcDemoApplication.class, args);
	}


	@Override
	public void run(String... strings) throws Exception {

		runQuery(strings);

		log.info("Creating tables");

		jdbcTemplate.execute("DROP TABLE IF EXISTS clients ");
		jdbcTemplate.execute("CREATE TABLE clients(" +
				"id serial PRIMARY KEY," +
				"first_name VARCHAR(50)," +
				"last_name VARCHAR(50),"+
				"created_on TIMESTAMP NOT NULL)");

		List<Object[]> splitUpNames = Arrays.asList("John Woo", "Jeff Dean", "Josh Bloch", "Josh Long").stream()
				.map(name -> name.split(" "))
				.collect(Collectors.toList());
		// Use a Java 8 stream to print out each tuple of the list
		splitUpNames.forEach(name -> log.info(String.format("Inserting client record for %s %s", name[0], name[1])));

		// Uses JdbcTemplate's batchUpdate operation to bulk load data
		jdbcTemplate.batchUpdate("INSERT INTO clients(first_name, last_name,created_on) VALUES (?,?,NOW())", splitUpNames);

		log.info("Querying for customer records where first_name = 'Josh':");
		runUpdateInsert(strings);
		runDelete(strings);
	}


	public void runUpdateInsert(String... strings) throws SQLException {
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			connection = DriverManager.getConnection(url, username, password);
			statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			resultSet = statement.executeQuery("select * from clients");
			System.out.println("Total number of returned rows :" + resultSet.getRow());
			while(resultSet.next()){
				System.out.println(resultSet.getString("first_name")+ " "+ resultSet.getString("last_name")+ " " + resultSet.getTimestamp("created_on"));
			}

			resultSet.beforeFirst();

			resultSet.absolute(2);

			resultSet.updateString("last_name","O'Connell");
			resultSet.updateRow();

			resultSet.moveToInsertRow();
			resultSet.updateString("first_name","Captain");
			resultSet.updateString("last_name","Obvious");

			Date now = new Date();
			long time = now.getTime();

			resultSet.updateTimestamp("created_on", new Timestamp(time));
			resultSet.insertRow();

		} catch (SQLException e) {
			System.err.println(e.getErrorCode());
		} finally {
			connection.close();
			statement.close();
			resultSet.close();
		}

	}

	public void runQuery(String... strings) throws SQLException {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try {
			connection = DriverManager.getConnection(url, username, password);

			String sql = "SELECT first_name FROM clients where first_name = ?";

			preparedStatement = connection.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			preparedStatement.setString(1,"Josh");

			resultSet = preparedStatement.executeQuery();
			resultSet.beforeFirst();
			while(resultSet.next()){
				System.out.println(resultSet.getString("first_name"));
			}

			preparedStatement.setString(1,"John");

			resultSet = preparedStatement.executeQuery();
			resultSet.beforeFirst();
			while(resultSet.next()){
				System.out.println(resultSet.getString("first_name"));
			}

		} catch (SQLException e) {
			System.err.println(e.getErrorCode());
		} finally {
			connection.close();
			preparedStatement.close();
			resultSet.close();
		}

	}

	public void runDelete(String... strings) throws SQLException {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try {
			connection = DriverManager.getConnection(url, username, password);

			String sql = "DELETE FROM clients where first_name = ?";

			preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1,"Josh");

			int result = preparedStatement.executeUpdate();
			if(result == 2){
				System.out.println("Successfully deleted");
			}

		} catch (SQLException e) {
			System.err.println(e.getErrorCode());
		} finally {
			connection.close();
			preparedStatement.close();
			resultSet.close();
		}

	}

}
