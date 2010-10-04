package net.dataforte.cassandra.pool;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Cassandra.Iface;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConnectionPoolTest {

	private static EmbeddedCassandraService cassandra;

	/**
	 * Set embedded cassandra up and spawn it in a new thread.
	 * 
	 * @throws TTransportException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@BeforeClass
	public static void setup() throws TTransportException, IOException, InterruptedException {
		// Tell cassandra where the configuration files are.
		// Use the test configuration file.
		URL resource = Thread.currentThread().getContextClassLoader().getResource("storage-conf.xml");
		String configPath = resource.getPath().substring(0, resource.getPath().lastIndexOf(File.separatorChar));
		System.out.println(configPath);
		System.setProperty("storage-config", configPath);

		CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
		cleaner.prepare();
		cassandra = new EmbeddedCassandraService();
		cassandra.init();
		Thread t = new Thread(cassandra);
		t.setDaemon(true);
		t.start();
	}

	@Test
	public void testConnectionPool() throws Exception {
		PoolConfiguration prop = new PoolProperties();
		prop.setHost("localhost");
		prop.setInitialSize(2);
		prop.setMinIdle(1);
		prop.setMaxIdle(4);
		prop.setMaxActive(4);
		ConnectionPool pool = new ConnectionPool(prop);
		
		Assert.assertEquals(0, pool.getActive());
		Assert.assertEquals(2, pool.getIdle());
		Assert.assertEquals(2, pool.getSize());
		
		// Get a connection
		Iface connection = pool.getConnection();
		Assert.assertNotNull(connection);
		
		Assert.assertEquals(1, pool.getActive());
		Assert.assertEquals(1, pool.getIdle());
		Assert.assertEquals(2, pool.getSize());
		
		// Release the connection
		pool.release(connection);
		
		Assert.assertEquals(0, pool.getActive());
		Assert.assertEquals(2, pool.getIdle());
		Assert.assertEquals(2, pool.getSize());
		
		pool.close();
	}
	
	@Test
	public void testAbandoned() throws Exception {
		PoolConfiguration prop = new PoolProperties();
		prop.setHost("localhost");
		prop.setInitialSize(2);
		prop.setMinIdle(1);
		prop.setMaxIdle(4);
		prop.setMaxActive(4);
		prop.setRemoveAbandonedTimeout(3);
		prop.setLogAbandoned(true);
		ConnectionPool pool = new ConnectionPool(prop);
		
		// Get a connection
		Iface connection = pool.getConnection();
		Assert.assertNotNull(connection);
		
		Assert.assertEquals(1, pool.getActive());
		Assert.assertEquals(1, pool.getIdle());
		Assert.assertEquals(2, pool.getSize());
		
		Thread.sleep(5000);
		pool.checkAbandoned();
		
		// The connection should now be closed
		Assert.assertEquals(0, pool.getActive());
		Assert.assertEquals(1, pool.getIdle());
		Assert.assertEquals(1, pool.getSize());
				
		try {
			connection.describe_cluster_name();
			Assert.fail("Connection should have been closed by checkAbandoned()");
		} catch (TTransportException e) {
			// Expected exception
		}
		pool.close();		
	}

	@Test
	public void testRing() throws Exception {
		PoolConfiguration prop = new PoolProperties();
		prop.setHost("localhost");
		prop.setAutomaticHostDiscovery(true);
		ConnectionPool pool = new ConnectionPool(prop);
		// Get a connection
		Iface connection = pool.getConnection();
		Assert.assertNotNull(connection);
		CassandraRing cassandraRing = pool.getCassandraRing();
		
		cassandraRing.refresh(connection);
		
		String[] hosts = cassandraRing.getHosts();
		Assert.assertNotNull(hosts);
		Assert.assertEquals(1, hosts.length);		
	}
}
