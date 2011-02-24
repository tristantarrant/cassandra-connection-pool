package net.dataforte.cassandra.pool;

import java.io.IOException;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;

public class EmbeddedServerHelper extends CleanupHelper {
	
	private static EmbeddedCassandraService cassandra;

	@BeforeClass
	public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException {
		System.setProperty("log4j.configuration", "log4j.properties");
		cassandra = new EmbeddedCassandraService();
		cassandra.start();
	}

}
