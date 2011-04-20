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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.thrift.Cassandra;
import org.junit.Assert;
import org.junit.Test;

public class DataSourceTest extends BaseEmbededServerSetupTest {

	@Test
	public void testDataSource() throws Exception {
		DataSource ds = new DataSource();
		ds.setHost("localhost");
		ds.setPort(DatabaseDescriptor.getRpcPort());
		
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
