package com.example.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class QueryClient {
        private Cluster cluster;
        private Session session;

        public void connect(String node) {
                cluster = Cluster.builder()
                        .addContactPoint(node)
                        .build();
                
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n",
                metadata.getClusterName());
                for ( Host host : metadata.getAllHosts() ) {
                        System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                                host.getDatacenter(), host.getAddress(),
                                host.getRack());
                }
                
                session = cluster.connect();
        }
        
        public void queryData() {
        	
        	
        	ResultSet results = session.execute("SELECT * FROM demodb.users " +
        			"WHERE user_name = 'JavaDude';");


        	System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "user_name",
        			"birth_year", "country",
        			"-------------------------------+-----------------------+--------------------"));
        			for (Row row : results) {
        			System.out.println(String.format("%-30s\t%-20s\t%-20s",
        			row.getString("user_name"),
        			row.getInt("birth_year"), row.getString("country")));
        			}
        			System.out.println();
        		

        }

        public void close() {
                cluster.close();
        }
        
        public Session getSession() {
        	return session;
        }

public static void main(String[] args) {
	QueryClient client = new QueryClient();
        client.connect("162.13.141.103");
        
        client.queryData();
        
        client.close();
        }
}
