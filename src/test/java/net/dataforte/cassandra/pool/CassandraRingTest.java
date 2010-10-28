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

import java.util.Random;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.junit.Ignore;

@Ignore
public class CassandraRingTest {
	
	static DataSource ds;

	public static final void main(String args[]) throws Exception {
		
		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				if(ds!=null) {
					System.out.println("Shutting down...");
					ds.close();
				}
			}
			
		});
		
		PoolConfiguration prop = new PoolProperties();
		prop.setHost("192.168.56.241");
		prop.setInitialSize(2);
		prop.setMinIdle(1);
		prop.setMaxIdle(4);
		prop.setMaxActive(4);
		prop.setRemoveAbandonedTimeout(3);
		prop.setLogAbandoned(true);
		prop.setJmxEnabled(true);
		prop.setAutomaticHostDiscovery(true);
		prop.setTestOnBorrow(true);
		prop.setTestOnReturn(true);
		
		ds = new DataSource(prop);
		Random random = new Random();
		for(;;) {
			Client connections[] = new Client[random.nextInt(prop.getMaxActive()-1)+1];
			System.out.println("Getting "+connections.length+" connections...");
			for(int i=0; i<connections.length; i++) {
				connections[i] = ds.getConnection();
			}
			Thread.sleep(1000);
			for(int i=0; i<connections.length; i++) {
				ds.releaseConnection(connections[i]);
			}
		
			Thread.sleep(6000);
		}
	}
}
