package com.example.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class QueryClientWithConsistency extends QueryClient {

	public QueryClientWithConsistency() {
		super();
	}

	public Statement generateSimpleStatement() {
		return new SimpleStatement("SELECT * FROM demodb.users "
				+ "WHERE user_name = 'JavaDude';"); 
	}

	public Statement generateStatementUsingBuilder() {
		return QueryBuilder
				.select()
				.all()
				.from("demodb", "users").where(QueryBuilder.eq("user_name","JavaDude"));
	}

	public void queryDataUsingStatement() {
		// Step 0 (initialisation step).
		// TODO Add the getSession() function to QueryClient that returns
		//      the session object created. We will re-use this function.
		// ADD THE BELOW TO QUERY CLIENT...
		// public Session getSession() {
		//	    return session;
		//	}

		
		// Step 2.
		// TODO Generate a statement object using either of the methods above.
		Statement s = generateSimpleStatement();
		// Step 3.
		// TODO Set a consistency level of 3 on the statement.
		s.setConsistencyLevel(ConsistencyLevel.THREE);
		// Step 4.
		// TODO Add a try catch block to gracefully handle the query execution exception thrown when executing the statement with
		// consistency level 3.
		// Step 5.
		// Replace the statement string with your statement object.
		// TODO Set a DowngradingConsistencyRetryPolicy on the statement.
		try {
				s.setRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE);
				ResultSet results = getSession().execute(s);
	
	
				System.out
				.println(String
						.format("%-30s\t%-20s\t%-20s\n%s", "user_name",
								"birth_year", "country",
								"-------------------------------+-----------------------+--------------------"));
				for (Row row : results) {
					System.out.println(String.format("%-30s\t%-20s\t%-20s",
							row.getString("user_name"), row.getInt("birth_year"),
							row.getString("country")));
				}
				System.out.println();
		} catch (UnavailableException uex) {
			System.out.println(uex.getMessage());
		}
	}

	private void printConsistencyLevel() {
		// Step 1.
		// TODO Print to console the default consistency level that the cluster object uses.
		System.out.println("Consistency Level: " + getSession().getCluster()
															   .getConfiguration()
															   .getQueryOptions()
															   .getConsistencyLevel());
	}

	public static void main(String[] args) {
		QueryClientWithConsistency client = new QueryClientWithConsistency();

		client.connect("162.13.141.103");

		client.printConsistencyLevel(); //Step 1.

		client.queryData();

		client.queryDataUsingStatement(); //Steps 2..5.

		client.close();
	}
}
