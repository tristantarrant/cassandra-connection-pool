package org.apache.cassandra;

import org.apache.cassandra.thrift.Cassandra;

public interface CassandraDataSource {
	Cassandra.Iface getConnection();
}
