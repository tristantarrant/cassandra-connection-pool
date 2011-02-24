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

import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

public class ConnectionPoolTest extends EmbeddedServerHelper {

	

	@Test
	public void testConnectionPool() throws Exception {

		PoolConfiguration prop = new PoolProperties();
		
		prop.setHost("127.0.0.1");
		prop.setPort(DatabaseDescriptor.getRpcPort());
		prop.setInitialSize(2);
		prop.setMinIdle(1);
		prop.setMaxIdle(4);
		prop.setMaxActive(4);
		ConnectionPool pool = new ConnectionPool(prop);

		Assert.assertEquals(0, pool.getActive());
		Assert.assertEquals(2, pool.getIdle());
		Assert.assertEquals(2, pool.getSize());

		// Get a connection
		Cassandra.Client connection = pool.getConnection();
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
		prop.setHost("127.0.0.1");
		prop.setPort(DatabaseDescriptor.getRpcPort());
		prop.setInitialSize(2);
		prop.setMinIdle(1);
		prop.setMaxIdle(4);
		prop.setMaxActive(4);
		prop.setRemoveAbandonedTimeout(3);
		prop.setLogAbandoned(true);
		ConnectionPool pool = new ConnectionPool(prop);

		// Get a connection
		Cassandra.Client connection = pool.getConnection();
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
		prop.setHost("127.0.0.1");
		prop.setPort(DatabaseDescriptor.getRpcPort());
		prop.setAutomaticHostDiscovery(true);
		ConnectionPool pool = new ConnectionPool(prop);
		// Get a connection
		Cassandra.Client connection = pool.getConnection();
		Assert.assertNotNull(connection);
		CassandraRing cassandraRing = pool.getCassandraRing();

		cassandraRing.refresh(connection);

		List<CassandraHost> hosts = cassandraRing.getHosts();
		Assert.assertNotNull(hosts);
		Assert.assertEquals(1, hosts.size());
	}
}
