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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.thrift.TException;

/**
 * Represents the Cassandra ring (all of the hosts in the cluster)
 * 
 * @author Tristan Tarrant
 *
 */
public class CassandraRing {
	private HostCyclePolicy policy;
	private Random random = new Random();
	private Map<String, CassandraHost> hosts;

	public CassandraRing(String hosts[]) {
		this(hosts, HostCyclePolicy.RANDOM);
	}

	public CassandraRing(String hosts[], HostCyclePolicy policy) {
		this.policy = policy;
		this.hosts = hostArrayToMap(hosts);
	}
	
	/**
	 * Maintain the host map, preserving any information about previously known hosts
	 * 
	 * @param hostAddresses
	 * @return 
	 */
	private Map<String, CassandraHost> hostArrayToMap(String hostAddresses[]) {
		Map<String, CassandraHost> hostsMap = new HashMap<String, CassandraHost>();
		for(String hostAddress : hostAddresses) {
			CassandraHost host = hosts==null?null:hosts.get(hostAddress);
			if(host==null) {
				host = new CassandraHost(hostAddress);
			}
			hostsMap.put(hostAddress, host);
		}
		return hostsMap;
	}

	public synchronized void refresh(Cassandra.Iface connection) throws TException, InvalidRequestException {

		// Obtain a set of available keyspaces
		Set<String> ks = connection.describe_keyspaces();
		String keyspace = null;
		for (String k : ks) {
			if (!"system".equalsIgnoreCase(k)) {
				keyspace = k;
				break;
			}
		}
		// Get a token range for the keyspace
		List<TokenRange> ranges = connection.describe_ring(keyspace);
		Set<String> addresses = new HashSet<String>();
		// Cycle all of the token ranges adding the endpoint addresses to a
		// set, so that duplicates are discarded
		for (TokenRange range : ranges) {
			addresses.addAll(range.getEndpoints());
		}
		this.hosts = hostArrayToMap(addresses.toArray(new String[] {}));

	}

	public List<CassandraHost> getHosts() {
		// Returns a list of hosts ordered according to the policy
		switch (this.policy) {
		case RANDOM:
			List<CassandraHost> list = new ArrayList<CassandraHost>(hosts.values());
			Collections.shuffle(list, random);
			return list;
		case ROUND_ROBIN:
		default:
			return new OffsetArrayList<CassandraHost>(hosts.values(), random.nextInt(hosts.size()));
		}
	}

	@Override
	public String toString() {
		return "CassandraRing [policy=" + policy + ", activeHosts=" + hosts.values() + "]";
	}

}
