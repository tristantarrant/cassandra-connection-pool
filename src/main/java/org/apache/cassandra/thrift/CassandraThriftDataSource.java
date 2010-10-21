package org.apache.cassandra.thrift;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.TException;

public interface CassandraThriftDataSource {
	Cassandra.Client getConnection() throws TException;
	void releaseConnection(Cassandra.Client connection);
}
