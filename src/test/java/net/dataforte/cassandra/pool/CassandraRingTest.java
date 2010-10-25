package net.dataforte.cassandra.pool;

import java.util.Random;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.junit.Ignore;

@Ignore
public class CassandraRingTest {
	
	static DataSource ds;

	public static final void main(String args[]) throws Exception {
		
		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				if(ds!=null) {
					System.out.println("Shutting down...");
					ds.close();
				}
			}
			
		});
		
		PoolConfiguration prop = new PoolProperties();
		prop.setHost("192.168.56.241");
		prop.setInitialSize(2);
		prop.setMinIdle(1);
		prop.setMaxIdle(4);
		prop.setMaxActive(4);
		prop.setRemoveAbandonedTimeout(3);
		prop.setLogAbandoned(true);
		prop.setJmxEnabled(true);
		prop.setAutomaticHostDiscovery(true);
		prop.setTestOnBorrow(true);
		prop.setTestOnReturn(true);
		
		ds = new DataSource(prop);
		Random random = new Random();
		for(;;) {
			Client connections[] = new Client[random.nextInt(prop.getMaxActive()-1)+1];
			System.out.println("Getting "+connections.length+" connections...");
			for(int i=0; i<connections.length; i++) {
				connections[i] = ds.getConnection();
			}
			Thread.sleep(1000);
			for(int i=0; i<connections.length; i++) {
				ds.releaseConnection(connections[i]);
			}
		
			Thread.sleep(6000);
		}
	}
}
