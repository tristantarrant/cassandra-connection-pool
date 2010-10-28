/**
 * Copyright 2010 Tristan Tarrant
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dataforte.cassandra.pool;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataSourceTest {

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
	
	@AfterClass
	public static void tearDown() {
		System.exit(0);
	}

	@Test
	public void testDataSource() throws Exception {
		DataSource ds = new DataSource();
			
		ds.setHost("localhost");
		ds.setInitialSize(2);
		ds.setMinIdle(1);
		ds.setMaxIdle(4);
		ds.setMaxActive(4);
		
		
		Assert.assertEquals(0, ds.getPool().getActive());
		Assert.assertEquals(2, ds.getPool().getIdle());
		Assert.assertEquals(2, ds.getPool().getSize());
		
		// Get a connection
		Cassandra.Client connection = ds.getConnection();
		Assert.assertNotNull(connection);
		
		Assert.assertEquals(1, ds.getPool().getActive());
		Assert.assertEquals(1, ds.getPool().getIdle());
		Assert.assertEquals(2, ds.getPool().getSize());
		
		// Release the connection
		ds.releaseConnection(connection);
		
		Assert.assertEquals(0, ds.getPool().getActive());
		Assert.assertEquals(2, ds.getPool().getIdle());
		Assert.assertEquals(2, ds.getPool().getSize());
		
		ds.close();
	}
}
