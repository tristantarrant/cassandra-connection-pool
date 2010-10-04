package net.dataforte.cassandra.pool;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TokenRange;

public class CassandraRing {
	public enum Policy {
		ROUND_ROBIN, 
		RANDOM
	};
	
	private Policy policy;
	private Random random = new Random();
	private String[] activeHosts;
	private AtomicInteger currentHost = new AtomicInteger(0);
	
	public CassandraRing(String hosts[]) {
		this(hosts, Policy.RANDOM);
	}
	
	public CassandraRing(String hosts[], Policy policy) {
		this.policy = policy;
		this.activeHosts = hosts;
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
			// Cycle all of the token ranges adding the endpoint addresses to a set, so that duplicates are discarded
			for (TokenRange range : ranges) {
				addresses.addAll(range.getEndpoints());
			}
			activeHosts = addresses.toArray(new String[] {});
		} catch (Exception e) {
		}
	}
	
	public String[] getHosts() {
		return activeHosts;
	}
	
	public String getHost() {
		switch(policy) {
		case RANDOM:
			return activeHosts[random.nextInt(activeHosts.length)];			
		default:
			return activeHosts[currentHost.getAndIncrement() % activeHosts.length];
		}
	}

}
