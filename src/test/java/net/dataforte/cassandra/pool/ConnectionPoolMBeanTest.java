package net.dataforte.cassandra.pool;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

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
		mBeanServer.registerMBean(pool.getJmxPool(), new ObjectName("cassandra.pool:type=ConnectionPool"));
		
		Thread.sleep(1000*500);
	}

}
