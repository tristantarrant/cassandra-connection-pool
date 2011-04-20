package net.dataforte.cassandra.pool;

import java.io.IOException;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Taken from Hector (MIT license). 
 * 
 * Copyright (c) 2010 Nate McCall
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 */
public abstract class BaseEmbededServerSetupTest {

	private static EmbeddedServerHelper embedded;

	protected String clusterName = "TestCluster";

	/**
	 * Set embedded cassandra up and spawn it in a new thread.
	 * 
	 * @throws TTransportException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@BeforeClass
	public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException {
		embedded = new EmbeddedServerHelper();
		embedded.setup();
	}

	@AfterClass
	public static void teardown() throws IOException {
		EmbeddedServerHelper.teardown();
		embedded = null;
	}

}
