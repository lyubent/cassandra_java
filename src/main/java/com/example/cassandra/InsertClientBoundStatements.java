package com.example.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;

public class InsertClientBoundStatements extends InsertClient {
	
	private PreparedStatement statement;
	private BoundStatement boundStatement;
	
	@Override
	public void loadData() {
		System.out.println("Insert data using a bound statement.");
		// TODO Bind some values to the prepared statement and execute the bound statement.
		getSession().execute(prepareStatement().bind("TJ", "independence", "M", "USA", 1776));
		
	}
	
	private BoundStatement prepareStatement() {
		// TODO Add the getSession() to the insert client the same way
        //      as the query client.
		statement = getSession().prepare(
				"INSERT INTO demodb.users (user_name, password, gender, country, birth_year) " +
				"VALUES (?, ?, ?, ?, ?);");
		return new BoundStatement(statement);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		InsertClientBoundStatements client = new InsertClientBoundStatements();
		
		client.connect("162.13.141.103");
		
		client.loadData();
		
		client.close();
	}

}
