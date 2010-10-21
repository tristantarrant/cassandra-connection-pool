package net.dataforte.cassandra.pool;

import org.junit.Assert;
import org.junit.Test;

public class PoolConfigurationProxyTest {

	@Test
	public void testPoolConfigurationProxy() {
		PoolProperties props = new PoolProperties();
		PoolConfiguration poolConfigurationProxy = PoolConfigurationProxy.makePoolConfigurationProxy(props);
		
		poolConfigurationProxy.setUsername("john.smith");
		
		Assert.assertEquals("john.smith", props.getUsername());
		
		poolConfigurationProxy.set("username", "mike.black");	
		
		Assert.assertEquals("mike.black", props.getUsername());
		
		poolConfigurationProxy.set("abandonWhenPercentageFull", "75");
		
		Assert.assertEquals(75, props.getAbandonWhenPercentageFull());
			
		poolConfigurationProxy.set("failoverPolicy", "ON_FAIL_TRY_ONE_NEXT_AVAILABLE");
		
		Assert.assertEquals(HostFailoverPolicy.ON_FAIL_TRY_ONE_NEXT_AVAILABLE, props.getFailoverPolicy());
	}
}
