package net.dataforte.cassandra.pool;

import org.junit.Assert;
import org.junit.Test;

public class URLPatternTest {
	
	@Test
	public void testUrlPattern() {
		PoolProperties properties = new PoolProperties();
		properties.setUrl("cassandra:thrift://myhost:9180");
		Assert.assertEquals("myhost", properties.getHost());
		Assert.assertEquals(9180, properties.getPort());
		properties.setUrl("cassandra:thrift://thathost");
		Assert.assertEquals("thathost", properties.getHost());
		Assert.assertEquals(9160, properties.getPort());
	}

}
