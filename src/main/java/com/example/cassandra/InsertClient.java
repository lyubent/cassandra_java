package com.example.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class InsertClient {
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
        
        public void loadData() {
        	session.execute(
        			"INSERT INTO demodb.users (user_name, password, gender, country, birth_year) " +
        			"VALUES (" +
        			"'JavaDude'," + "'javapassword'," +
        			"'M'," +
        			"'Peru'," +
        			"30" +
        			");");
        }

        public void close() {
                cluster.close();
        }
        
        public Session getSession() {
        	return session;
        }

public static void main(String[] args) {
	InsertClient client = new InsertClient();
        client.connect("162.13.141.103");
        
        client.loadData();
        
        client.close();
        }
}
