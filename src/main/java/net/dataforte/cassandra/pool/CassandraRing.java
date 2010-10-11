package net.dataforte.cassandra.pool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TokenRange;

public class CassandraRing {
	public enum Policy {
		ROUND_ROBIN, RANDOM
	};

	private Policy policy;
	private Random random = new Random();
	private List<CassandraHost> activeHosts;

	public CassandraRing(String hosts[]) {
		this(hosts, Policy.RANDOM);
	}

	public CassandraRing(String hosts[], Policy policy) {
		this.policy = policy;
		this.activeHosts = hostArrayToList(hosts);
	}

	private List<CassandraHost> hostArrayToList(String hosts[]) {
		List<CassandraHost> list = new ArrayList<CassandraHost>();
		for (String host : hosts) {
			list.add(new CassandraHost(host));
		}
		return list;
	}

	public synchronized void refresh(Cassandra.Iface connection) {
		try {
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
			activeHosts = hostArrayToList(addresses.toArray(new String[] {}));
		} catch (Exception e) {
		}
	}

	public List<CassandraHost> getHosts() {
		// Returns a list of hosts ordered according to the policy
		ArrayList<CassandraHost> list = new ArrayList<CassandraHost>(activeHosts);
		switch(this.policy) {
		case RANDOM:			
			Collections.shuffle(list, random);
			return list;
		case ROUND_ROBIN:
		default:
			return list;
		}
	}

}
