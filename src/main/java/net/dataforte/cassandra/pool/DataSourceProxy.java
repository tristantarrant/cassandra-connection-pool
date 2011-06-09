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

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * The DataSource proxy lets us implements methods that don't exist in the
 * current CassandraThriftDataSource interface but might be methods that are 
 * part of a future CassandraThriftDataSource interface. <br/>
 * 
 * @author Tristan Tarrant
 */

public class DataSourceProxy {
	private static final Logger log = LoggerFactory.getLogger(DataSourceProxy.class);

	protected volatile ConnectionPool pool = null;

	protected PoolConfiguration poolProperties = null;

	public DataSourceProxy(PoolConfiguration poolProperties) {
		if (poolProperties == null)
			throw new NullPointerException("PoolConfiguration can not be null.");
		this.poolProperties = poolProperties;

	}

	public PoolConfiguration getPoolProperties() {
		return poolProperties;
	}

	/**
	 * Sets up the connection pool, by creating a pooling driver.
	 * 
	 * @return Driver
	 * @throws TException
	 */
	public synchronized ConnectionPool createPool() throws TException {
		if (pool != null) {
			return pool;
		} else {
			pool = new ConnectionPool(poolProperties);
			return pool;
		}
	}

	public Cassandra.Client getConnection() throws TException {
		if (pool == null)
			return createPool().getConnection();
		return pool.getConnection();
	}

	
	public void releaseConnection(Cassandra.Client connection) {
		if(connection == null)
			return;
		if(pool == null) 
			throw new IllegalStateException("Attempt to release a connection on an uninitialized pool");
		pool.release(connection);
	}

	public ConnectionPool getPool() throws TException {
		if (pool == null)
			return createPool();
		return pool;
	}

	public void close() {
		close(false);
	}

	public void close(boolean all) {
		try {
			if (pool != null) {
				final ConnectionPool p = pool;
				pool = null;
				if (p != null) {
					p.close(all);
				}
			}
		} catch (Exception x) {
			log.warn("Error closing connection pool.", x);
		}
	}

	public int getPoolSize() throws TException {
		final ConnectionPool p = pool;
		if (p == null)
			return 0;
		else
			return p.getSize();
	}

	@Override
	public String toString() {
		return super.toString() + "{" + getPoolProperties() + "}";
	}
}
