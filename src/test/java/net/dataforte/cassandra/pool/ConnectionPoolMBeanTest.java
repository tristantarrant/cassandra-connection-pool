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

import java.lang.management.ManagementFactory;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.junit.Assert;
import org.junit.Test;

public class ConnectionPoolMBeanTest {
	
	@Test
	public void testMBean() throws Exception {
		MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
		PoolConfiguration prop = new PoolProperties();
		prop.setHost("localhost");
		prop.setJmxEnabled(true);
		prop.setInitialSize(0);

		ConnectionPool pool = new ConnectionPool(prop);
		ObjectName name = new ObjectName("cassandra.pool:type=ConnectionPool");
		mBeanServer.registerMBean(pool.getJmxPool(), name);
		
		Set<ObjectInstance> mbeans = mBeanServer.queryMBeans(name, null);
		
		Assert.assertEquals(1, mbeans.size());
		ObjectInstance next = mbeans.iterator().next();
		
		Assert.assertEquals("net.dataforte.cassandra.pool.jmx.ConnectionPoolMBean", next.getClassName());
		
	}

}
