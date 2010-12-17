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

import java.sql.SQLException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of a connection pool for Cassandra Thrift connections. Supports reaping of abandoned connections
 * 
 * Derived from org.apache.tomcat.jdbc.pool.ConnectionPool by fhanik
 * 
 * @author Tristan Tarrant
 *
 */
public class ConnectionPool {
	/**
	 * Prefix type for JMX registration
	 */
	public static final String POOL_JMX_PREFIX = "cassandra.pool";
	public static final String POOL_JMX_TYPE_PREFIX = POOL_JMX_PREFIX+":type=";

	/**
	 * Logger
	 */
	private static final Logger log = LoggerFactory.getLogger(ConnectionPool.class);

	// ===============================================================================
	// INSTANCE/QUICK ACCESS VARIABLE
	// ===============================================================================
	/**
	 * Carries the size of the pool, instead of relying on a queue
	 * implementation that usually iterates over to get an exact count
	 */
	private AtomicInteger size = new AtomicInteger(0);

	/**
	 * All the information about the connection pool These are the properties
	 * the pool got instantiated with
	 */
	private PoolConfiguration poolProperties;

	/**
	 * Contains all the connections that are in use TODO - this shouldn't be a
	 * blocking queue, simply a list to hold our objects
	 */
	private BlockingQueue<PooledConnection> busy;

	/**
	 * Contains all the idle connections
	 */
	private BlockingQueue<PooledConnection> idle;
	
	private Map<Cassandra.Client, PooledConnection> connectionMap;

	/**
	 * The thread that is responsible for checking abandoned and idle threads and for keeping an up-to-date list of Cassandra hosts
	 */
	private volatile PoolMaintenance poolMaintenance;

	/**
	 * Pool closed flag
	 */
	private volatile boolean closed = false;

	/**
	 * reference to the JMX mbean
	 */
	protected net.dataforte.cassandra.pool.jmx.ConnectionPoolMBean jmxPool = null;

	/**
	 * counter to track how many threads are waiting for a connection
	 */
	private AtomicInteger waitcount = new AtomicInteger(0);
	
	/**
	 * the object which contains the list of active Cassandra nodes
	 */
	private CassandraRing cassandraRing = null;

	// ===============================================================================
	// PUBLIC METHODS
	// ===============================================================================

	/**
	 * Instantiate a connection pool. This will create connections if
	 * initialSize is larger than 0. The {@link PoolProperties} should not be
	 * reused for another connection pool.
	 * 
	 * @param prop
	 *            PoolProperties - all the properties for this connection pool
	 * @throws SQLException
	 */
	public ConnectionPool(PoolConfiguration prop) throws TException {
		// setup quick access variables and pools
		init(prop);
	}

	/**
	 * Borrows a connection from the pool. If a connection is available (in the
	 * idle queue) or the pool has not reached {@link PoolProperties#maxActive
	 * maxActive} connections a connection is returned immediately. If no
	 * connection is available, the pool will attempt to fetch a connection for
	 * {@link PoolProperties#maxWait maxWait} milliseconds.
	 * 
	 * @return Connection - a java.sql.Connection/javax.sql.PooledConnection
	 *         reflection proxy, wrapping the underlying object.
	 * @throws SQLException
	 *             - if the wait times out or a failure occurs creating a
	 *             connection
	 */
	public Cassandra.Client getConnection() throws TException {
		// check out a connection
		PooledConnection con = borrowConnection(-1);
		return con.getConnection();
	}

	/**
	 * Returns the name of this pool
	 * 
	 * @return String - the name of the pool
	 */
	public String getName() {
		return getPoolProperties().getPoolName();
	}

	/**
	 * Return the number of threads waiting for a connection
	 * 
	 * @return number of threads waiting for a connection
	 */
	public int getWaitCount() {
		return waitcount.get();
	}

	/**
	 * Returns the pool properties associated with this connection pool
	 * 
	 * @return PoolProperties
	 * 
	 */
	public PoolConfiguration getPoolProperties() {
		return this.poolProperties;
	}

	/**
	 * Returns the total size of this pool, this includes both busy and idle
	 * connections
	 * 
	 * @return int - number of established connections to the database
	 */
	public int getSize() {
		return size.get();
	}

	/**
	 * Returns the number of connections that are in use
	 * 
	 * @return int - number of established connections that are being used by
	 *         the application
	 */
	public int getActive() {
		return busy.size();
	}

	/**
	 * Returns the number of idle connections
	 * 
	 * @return int - number of established connections not being used
	 */
	public int getIdle() {
		return idle.size();
	}

	/**
	 * Returns true if {@link #close close} has been called, and the connection
	 * pool is unusable
	 * 
	 * @return boolean
	 */
	public boolean isClosed() {
		return this.closed;
	}

	
	public void close() {
		close(false);
	}

	/**
	 * Closes the pool and all disconnects all idle connections Active
	 * connections will be closed upon the {@link java.sql.Connection#close
	 * close} method is called on the underlying connection instead of being
	 * returned to the pool
	 * 
	 * @param force
	 *            - true to even close the active connections
	 */
	public void close(boolean force) {
		// are we already closed
		if (this.closed)
			return;
		// prevent other threads from entering
		this.closed = true;
		// stop background thread
		if (poolMaintenance != null) {
			poolMaintenance.stopRunning();
		}

		/* release all idle connections */
		BlockingQueue<PooledConnection> pool = (idle.size() > 0) ? idle : (force ? busy : idle);
		while (pool.size() > 0) {
			try {
				// retrieve the next connection
				PooledConnection con = pool.poll(1000, TimeUnit.MILLISECONDS);
				// close it and retrieve the next one, if one is available
				while (con != null) {
					// close the connection
					if (pool == idle)
						release(con);
					else
						abandon(con);
					con = pool.poll(1000, TimeUnit.MILLISECONDS);
				} // while
			} catch (InterruptedException ex) {
				Thread.interrupted();
			}
			if (pool.size() == 0 && force && pool != busy)
				pool = busy;
		}
		if (this.getPoolProperties().isJmxEnabled())
			this.jmxPool = null;
	} // closePool
	
	// ===============================================================================
	// PROTECTED METHODS
	// ===============================================================================
	

	/**
	 * Initialize the connection pool - called from the constructor
	 * 
	 * @param properties
	 *            PoolProperties - properties used to initialize the pool with
	 * @throws SQLException
	 *             if initialization fails
	 */
	protected void init(PoolConfiguration properties) throws TException {
		poolProperties = properties;
		
		connectionMap = new HashMap<Cassandra.Client, PooledConnection>();
		
		cassandraRing = new CassandraRing(poolProperties.getConfiguredHosts());
		
		busy = new ArrayBlockingQueue<PooledConnection>(properties.getMaxActive(), false);

		if (properties.isFairQueue()) {
			idle = new FairBlockingQueue<PooledConnection>();			
		} else {
			idle = new ArrayBlockingQueue<PooledConnection>(properties.getMaxActive(), properties.isFairQueue());
		}

		// if the evictor thread is supposed to run, start it now
		if (properties.isPoolSweeperEnabled()) {
			if(log.isDebugEnabled()) {
				log.debug("Starting pool maintenance thread");
			}
			poolMaintenance = new PoolMaintenance("[Pool-Maintenance]:" + properties.getName(), this, properties.getTimeBetweenEvictionRunsMillis());
			poolMaintenance.start();
		} // end if

		// make sure the pool is properly configured
		if (properties.getMaxActive() < properties.getInitialSize()) {
			log.warn("initialSize is larger than maxActive, setting initialSize to: " + properties.getMaxActive());
			properties.setInitialSize(properties.getMaxActive());
		}
		if (properties.getMinIdle() > properties.getMaxActive()) {
			log.warn("minIdle is larger than maxActive, setting minIdle to: " + properties.getMaxActive());
			properties.setMinIdle(properties.getMaxActive());
		}
		if (properties.getMaxIdle() > properties.getMaxActive()) {
			log.warn("maxIdle is larger than maxActive, setting maxIdle to: " + properties.getMaxActive());
			properties.setMaxIdle(properties.getMaxActive());
		}
		if (properties.getMaxIdle() < properties.getMinIdle()) {
			log.warn("maxIdle is smaller than minIdle, setting maxIdle to: " + properties.getMinIdle());
			properties.setMaxIdle(properties.getMinIdle());
		}

		// create JMX MBean
		if (this.getPoolProperties().isJmxEnabled()) {
			if(log.isDebugEnabled()) {
				log.debug("Creating JMX MBean");
			}
			createMBean();
		}

		// initialize the pool with its initial set of members
		PooledConnection[] initialPool = new PooledConnection[poolProperties.getInitialSize()];
		try {
			for (int i = 0; i < initialPool.length; i++) {
				initialPool[i] = this.borrowConnection(0); // don't wait, should
															// be no contention
			} // for

		} catch (TException x) {
			if (jmxPool != null)
				jmxPool.notify(net.dataforte.cassandra.pool.jmx.ConnectionPoolMBean.NOTIFY_INIT, getStackTrace(x));
			close(true);
			throw x;
		} finally {
			// return the members as idle to the pool
			for (int i = 0; i < initialPool.length; i++) {
				if (initialPool[i] != null) {
					try {
						this.returnConnection(initialPool[i]);
					} catch (Exception x) {/* NOOP */
					}
				} // end if
			} // for
		} // catch

		closed = false;
		if(log.isInfoEnabled()) {
			log.info("ConnectionPool initialized.");
		}
		if(log.isDebugEnabled()) {
			for(String p : PoolProperties.getPropertyNames()) {
				log.debug("ConnectionPool: "+p+"="+poolProperties.get(p));
			}
		}
	}

	// ===============================================================================
	// CONNECTION POOLING IMPL LOGIC
	// ===============================================================================

	/**
	 * thread safe way to abandon a connection signals a connection to be
	 * abandoned. this will disconnect the connection, and log the stack trace
	 * if logAbanded=true
	 * 
	 * @param con
	 *            PooledConnection
	 */
	protected void abandon(PooledConnection con) {
		if (con == null)
			return;
		try {
			con.lock();
			String trace = con.getStackTrace();
			if (getPoolProperties().isLogAbandoned()) {
				log.warn("Connection has been abandoned " + con + ":" + trace);
			}
			if (jmxPool != null) {
				jmxPool.notify(net.dataforte.cassandra.pool.jmx.ConnectionPoolMBean.NOTIFY_ABANDON, trace);
			}
			// release the connection
			release(con);
			// we've asynchronously reduced the number of connections
			// we could have threads stuck in idle.poll(timeout) that will never
			// be notified
			if (waitcount.get() > 0)
				idle.offer(new PooledConnection(poolProperties, this));
		} finally {
			con.unlock();
		}
	}

	/**
	 * thread safe way to abandon a connection signals a connection to be
	 * abandoned. this will disconnect the connection, and log the stack trace
	 * if logAbanded=true
	 * 
	 * @param con
	 *            PooledConnection
	 */
	protected void suspect(PooledConnection con) {
		if (con == null)
			return;
		if (con.isSuspect())
			return;
		try {
			con.lock();
			String trace = con.getStackTrace();
			if (getPoolProperties().isLogAbandoned()) {
				log.warn("Connection has been marked suspect, possibly abandoned " + con + "[" + (System.currentTimeMillis() - con.getTimestamp()) + " ms.]:"
						+ trace);
			}
			if (jmxPool != null) {
				jmxPool.notify(net.dataforte.cassandra.pool.jmx.ConnectionPoolMBean.SUSPECT_ABANDONED_NOTIFICATION, trace);
			}
			con.setSuspect(true);
		} finally {
			con.unlock();
		}
	}

	/**
	 * thread safe way to release a connection
	 * 
	 * @param con
	 *            PooledConnection
	 */
	protected void release(PooledConnection con) {
		if (con == null)
			return;
		try {
			con.lock();
			if (con.release()) {
				// counter only decremented once
				size.addAndGet(-1);
			}
		} finally {
			con.unlock();
		}
	}

	/**
	 * Thread safe way to retrieve a connection from the pool
	 * 
	 * @param wait
	 *            - time to wait, overrides the maxWait from the properties, set
	 *            to -1 if you wish to use maxWait, 0 if you wish no wait time.
	 * @return PooledConnection
	 * @throws SQLException
	 */
	private PooledConnection borrowConnection(int wait) throws TException {

		if (isClosed()) {
			throw new TException("Connection pool closed.");
		} // end if

		// get the current time stamp
		long now = System.currentTimeMillis();
		// see if there is one available immediately
		PooledConnection con = idle.poll();

		while (true) {
			if (con != null) {
				// configure the connection and return it
				PooledConnection result = borrowConnection(now, con);
				// null should never be returned, but was in a previous impl.
				if (result != null)
					return result;
			}

			// if we get here, see if we need to create one
			// this is not 100% accurate since it doesn't use a shared
			// atomic variable - a connection can become idle while we are
			// creating
			// a new connection
			if (size.get() < getPoolProperties().getMaxActive()) {
				// atomic duplicate check
				if (size.addAndGet(1) > getPoolProperties().getMaxActive()) {
					// if we got here, two threads passed through the first if
					size.decrementAndGet();
				} else {
					// create a connection, we're below the limit
					return createConnection(now, con);
				}
			} // end if

			// calculate wait time for this iteration
			long maxWait = wait;
			// if the passed in wait time is -1, means we should use the pool
			// property value
			if (wait == -1) {
				maxWait = (getPoolProperties().getMaxWait() <= 0) ? Long.MAX_VALUE : getPoolProperties().getMaxWait();
			}

			long timetowait = Math.max(0, maxWait - (System.currentTimeMillis() - now));
			waitcount.incrementAndGet();
			try {
				// retrieve an existing connection
				con = idle.poll(timetowait, TimeUnit.MILLISECONDS);
			} catch (InterruptedException ex) {
				Thread.interrupted();// clear the flag, and bail out
				TException sx = new TException("Pool wait interrupted.");
				sx.initCause(ex);
				throw sx;
			} finally {
				waitcount.decrementAndGet();
			}
			if (maxWait == 0 && con == null) { // no wait, return one if we have
												// one
				throw new TException("[" + getName() + "] " + "NoWait: Pool empty. Unable to fetch a connection, none available["
						+ busy.size() + " in use].");
			}
			// we didn't get a connection, lets see if we timed out
			if (con == null) {
				if ((System.currentTimeMillis() - now) >= maxWait) {
					if(log.isDebugEnabled()) {
						int counter = 0;
						for(Iterator<PooledConnection> i=busy.iterator(); i.hasNext(); ) {
							PooledConnection connection = i.next();
							log.debug("Busy connection "+counter+" borrowed at "+connection.getStackTrace());
							++counter;
						}
					}
					throw new TException("[" + getName() + "] " + "Timeout: Pool empty. Unable to fetch a connection in "
							+ (maxWait / 1000) + " seconds, none available[" + busy.size() + " in use].");
				} else {
					// no timeout, lets try again
					continue;
				}
			}
		} // while
	}

	/**
	 * Creates a Cassandra connection and tries to connect to the database.
	 * 
	 * @param now
	 *            timestamp of when this was called
	 * @param con
	 *            the previous pooled connection - argument not used
	 * @return a PooledConnection that has been connected
	 * @throws SQLException
	 */
	protected PooledConnection createConnection(long now, PooledConnection con) throws TException {
		// no connections where available we'll create one
		boolean error = false;
		try {
			// connect and validate the connection
			con = create();
			con.lock();
			con.connect();
			if (con.validate(PooledConnection.VALIDATE_INIT)) {
				connectionMap.put(con.getConnection(), con);
				// no need to lock a new one, its not contented
				con.setTimestamp(now);
				if (getPoolProperties().isLogAbandoned()) {
					con.setStackTrace(getThreadDump());
				}
				if (!busy.offer(con)) {
					log.debug("Connection doesn't fit into busy array, connection will not be traceable.");
				}				
				return con;
			} else {
				// validation failed, make sure we disconnect
				// and clean up
				error = true;
			} // end if
		} catch (Exception e) {
			error = true;
			if (log.isDebugEnabled())
				log.debug("Unable to create a new Cassandra connection.", e);
			if (e instanceof TException) {
				throw (TException) e;
			} else {
				TException ex = new TException(e.getMessage());
				ex.initCause(e);
				throw ex;
			}
		} finally {
			if (error) {
				release(con);
			}
			con.unlock();
		}// catch
		return null;
	}

	/**
	 * Validates and configures a previously idle connection
	 * 
	 * @param now
	 *            - timestamp
	 * @param con
	 *            - the connection to validate and configure
	 * @return con
	 * @throws SQLException
	 *             if a validation error happens
	 */
	protected PooledConnection borrowConnection(long now, PooledConnection con) throws TException {
		// we have a connection, lets set it up

		// flag to see if we need to nullify
		boolean setToNull = false;
		try {
			con.lock();

			if (con.isReleased()) {
				return null;
			}

			if (!con.isDiscarded() && !con.isInitialized()) {
				// attempt to connect
				con.connect();
			}
			if ((!con.isDiscarded()) && con.validate(PooledConnection.VALIDATE_BORROW)) {
				// set the timestamp
				con.setTimestamp(now);
				if (getPoolProperties().isLogAbandoned()) {
					// set the stack trace for this pool
					con.setStackTrace(getThreadDump());
				}
				if (!busy.offer(con)) {
					log.debug("Connection doesn't fit into busy array, connection will not be traceable.");
				}
				return con;
			}
			// if we reached here, that means the connection
			// is either discarded or validation failed.
			// we will make one more attempt
			// in order to guarantee that the thread that just acquired
			// the connection shouldn't have to poll again.
			try {
				con.reconnect();
				if (con.validate(PooledConnection.VALIDATE_INIT)) {
					// set the timestamp
					con.setTimestamp(now);
					if (getPoolProperties().isLogAbandoned()) {
						// set the stack trace for this pool
						con.setStackTrace(getThreadDump());
					}
					if (!busy.offer(con)) {
						log.debug("Connection doesn't fit into busy array, connection will not be traceable.");
					}
					return con;
				} else {
					// validation failed.
					release(con);
					setToNull = true;
					throw new TException("Failed to validate a newly established connection.");
				}
			} catch (Exception x) {
				release(con);
				setToNull = true;
				if (x instanceof TException) {
					throw (TException) x;
				} else {
					TException ex = new TException(x.getMessage());
					ex.initCause(x);
					throw ex;
				}
			}
		} finally {
			con.unlock();
			if (setToNull) {
				con = null;
			}
		}
	}

	/**
	 * Determines if a connection should be closed upon return to the pool.
	 * 
	 * @param con
	 *            - the connection
	 * @param action
	 *            - the validation action that should be performed
	 * @return true if the connection should be closed
	 */
	protected boolean shouldClose(PooledConnection con, int action) {
		if (con.isDiscarded())
			return true;
		if (isClosed())
			return true;
		if (!con.validate(action))
			return true;
		if (getPoolProperties().getMaxAge() > 0) {
			return (System.currentTimeMillis() - con.getLastConnected()) > getPoolProperties().getMaxAge();
		} else {
			return false;
		}
	}

	public void release(Cassandra.Client connection) {
		PooledConnection pooledConnection = connectionMap.get(connection);
		this.returnConnection(pooledConnection);
	}

	/**
	 * Returns a connection to the pool If the pool is closed, the connection
	 * will be released If the connection is not part of the busy queue, it will
	 * be released. If {@link PoolProperties#testOnReturn} is set to true it
	 * will be validated
	 * 
	 * @param con
	 *            PooledConnection to be returned to the pool
	 */
	protected void returnConnection(PooledConnection con) {
		if (isClosed()) {
			// if the connection pool is closed
			// close the connection instead of returning it
			release(con);
			return;
		} // end if

		if (con != null) {
			try {
				con.lock();

				if (busy.remove(con)) {

					if (!shouldClose(con, PooledConnection.VALIDATE_RETURN)) {
						con.setStackTrace(null);
						con.setTimestamp(System.currentTimeMillis());
						if (((idle.size() >= poolProperties.getMaxIdle()) && !poolProperties.isPoolSweeperEnabled()) || (!idle.offer(con))) {
							if (log.isDebugEnabled()) {
								log.debug("Connection [" + con + "] will be closed and not returned to the pool, idle[" + idle.size() + "]>=maxIdle["
										+ poolProperties.getMaxIdle() + "] idle.offer failed.");
							}
							release(con);
						}
					} else {
						if (log.isDebugEnabled()) {
							log.debug("Connection [" + con + "] will be closed and not returned to the pool.");
						}
						release(con);
					} // end if
				} else {
					if (log.isDebugEnabled()) {
						log.debug("Connection [" + con + "] will be closed and not returned to the pool, busy.remove failed.");
					}
					release(con);
				}
			} finally {
				con.unlock();
			}
		} // end if
	} // checkIn

	/**
	 * Determines if a connection should be abandoned based on
	 * {@link PoolProperties#abandonWhenPercentageFull} setting.
	 * 
	 * @return true if the connection should be abandoned
	 */
	protected boolean shouldAbandon() {
		if (poolProperties.getAbandonWhenPercentageFull() == 0)
			return true;
		float used = busy.size();
		float max = poolProperties.getMaxActive();
		float perc = poolProperties.getAbandonWhenPercentageFull();
		return (used / max * 100f) >= perc;
	}

	/**
	 * Iterates through all the busy connections and checks for connections that
	 * have timed out
	 */
	public void checkAbandoned() {
		if(log.isDebugEnabled()) {
			log.debug("["+getName()+"] checking for abandoned connections");
		}
		try {
			if (busy.size() == 0)
				return;
			Iterator<PooledConnection> locked = busy.iterator();
			int sto = getPoolProperties().getSuspectTimeout();
			while (locked.hasNext()) {
				PooledConnection con = locked.next();
				boolean setToNull = false;
				try {
					con.lock();
					// the con has been returned to the pool
					// ignore it
					if (idle.contains(con))
						continue;
					long time = con.getTimestamp();
					long now = System.currentTimeMillis();
					if (shouldAbandon() && (now - time) > con.getAbandonTimeout()) {
						busy.remove(con);
						abandon(con);
						setToNull = true;
					} else if (sto > 0 && (now - time) > (sto * 1000)) {
						suspect(con);
					} else {
						// do nothing
					} // end if
				} finally {
					con.unlock();
					if (setToNull)
						con = null;
				}
			} // while
		} catch (ConcurrentModificationException e) {
			log.debug("checkAbandoned failed.", e);
		} catch (Exception e) {
			log.warn("checkAbandoned failed, it will be retried.", e);
		}
	}

	/**
	 * Iterates through the idle connections and resizes the idle pool based on
	 * parameters {@link PoolProperties#maxIdle}, {@link PoolProperties#minIdle}
	 * , {@link PoolProperties#minEvictableIdleTimeMillis}
	 */
	public void checkIdle() {
		try {
			if (idle.size() == 0)
				return;
			long now = System.currentTimeMillis();
			Iterator<PooledConnection> unlocked = idle.iterator();
			while ((idle.size() >= getPoolProperties().getMinIdle()) && unlocked.hasNext()) {
				PooledConnection con = unlocked.next();
				boolean setToNull = false;
				try {
					con.lock();
					// the con been taken out, we can't clean it up
					if (busy.contains(con))
						continue;
					long time = con.getTimestamp();
					if ((con.getReleaseTime() > 0) && ((now - time) > con.getReleaseTime()) && (getSize() > getPoolProperties().getMinIdle())) {	
						if(log.isDebugEnabled()) {
							log.debug("Releasing idle connection "+con);
						}
						release(con);
						idle.remove(con);
						
						setToNull = true;
					} else {
						// do nothing
					} // end if
				} finally {
					con.unlock();
					if (setToNull)
						con = null;
				}
			} // while
		} catch (ConcurrentModificationException e) {
			log.debug("checkIdle failed.", e);
		} catch (Exception e) {
			log.warn("checkIdle failed, it will be retried.", e);
		}

	}

	/**
	 * Forces a validation of all idle connections if
	 * {@link PoolProperties#testWhileIdle} is set.
	 */
	public void testAllIdle() {
		try {
			if (idle.size() == 0)
				return;
			Iterator<PooledConnection> unlocked = idle.iterator();
			while (unlocked.hasNext()) {
				PooledConnection con = unlocked.next();
				try {
					con.lock();
					// the con been taken out, we can't clean it up
					if (busy.contains(con))
						continue;
					if (!con.validate(PooledConnection.VALIDATE_IDLE)) {
						idle.remove(con);
						release(con);
					}
				} finally {
					con.unlock();
				}
			} // while
		} catch (ConcurrentModificationException e) {
			log.debug("testAllIdle failed.", e);
		} catch (Exception e) {
			log.warn("testAllIdle failed, it will be retried.", e);
		}

	}
	
	/**
	 * Refreshes the ring information
	 */
	public void refreshRing() {
		try {
			if (idle.size() == 0)
				return;
			Iterator<PooledConnection> unlocked = idle.iterator();
			while (unlocked.hasNext()) {
				PooledConnection con = unlocked.next();
				try {
					con.lock();
					// the con been taken out, we can't use it
					if (busy.contains(con))
						continue;
					cassandraRing.refresh(con.getConnection());
					// we have successfully refreshed the ring, we can quit now
					log.debug("refreshRing success, ring = "+cassandraRing);
					return;
				} catch (TTransportException t) {
					// there was an error retrieving ring information from this connection, remove it
					log.warn("removing connection to non-responding host ");
					idle.remove(con);
					release(con);
				} finally {
					con.unlock();
				}
			} // while			
		} catch (ConcurrentModificationException e) {
			log.debug("refreshRing failed.", e);
		} catch (Exception e) {
			log.warn("refreshRing failed, it will be retried.", e);
		}

	}

	/**
	 * Creates a stack trace representing the existing thread's current state.
	 * 
	 * @return a string object representing the current state. TODO investigate
	 *         if we simply should store
	 *         {@link java.lang.Thread#getStackTrace()} elements
	 */
	protected static String getThreadDump() {
		Exception x = new Exception();
		x.fillInStackTrace();
		return getStackTrace(x);
	}

	/**
	 * Convert an exception into a String
	 * 
	 * @param x
	 *            - the throwable
	 * @return a string representing the stack trace
	 */
	public static String getStackTrace(Throwable x) {
		if (x == null) {
			return null;
		} else {
			java.io.ByteArrayOutputStream bout = new java.io.ByteArrayOutputStream();
			java.io.PrintStream writer = new java.io.PrintStream(bout);
			x.printStackTrace(writer);
			String result = bout.toString();
			return (x.getMessage() != null && x.getMessage().length() > 0) ? x.getMessage() + ";" + result : result;
		} // end if
	}

	/**
	 * Create a new pooled connection object. Not connected nor validated.
	 * 
	 * @return a pooled connection object
	 */
	protected PooledConnection create() {
		PooledConnection con = new PooledConnection(getPoolProperties(), this);		
		return con;
	}

	/**
	 * Hook to perform final actions on a pooled connection object once it has
	 * been disconnected and will be discarded
	 * 
	 * @param con
	 */
	protected void finalize(PooledConnection con) {
		
	}

	/**
	 * Hook to perform final actions on a pooled connection object once it has
	 * been disconnected and will be discarded
	 * 
	 * @param con
	 */
	protected void disconnectEvent(PooledConnection con, boolean finalizing) {
		connectionMap.remove(con.getConnection());
	}

	/**
	 * Return the object that is potentially registered in JMX for notifications
	 * 
	 * @return the object implementing the
	 *         {@link net.dataforte.cassandra.pool.jmx.ConnectionPoolMBean}
	 *         interface
	 */
	public net.dataforte.cassandra.pool.jmx.ConnectionPoolMBean getJmxPool() {
		return jmxPool;
	}

	public CassandraRing getCassandraRing() {
		return cassandraRing;
	}

	/**
	 * Create MBean object that can be registered.
	 */
	protected void createMBean() {
		try {
			jmxPool = new net.dataforte.cassandra.pool.jmx.ConnectionPoolMBean(this);
		} catch (Exception x) {
			log.warn("Unable to start JMX integration for connection pool. Instance[" + getName() + "] can't be monitored.", x);
		}
	}	

	protected class PoolMaintenance extends Thread {
		protected ConnectionPool pool;
		protected long sleepTime;
		protected volatile boolean run = true;

		PoolMaintenance(String name, ConnectionPool pool, long sleepTime) {
			super(name);
			this.setDaemon(true);
			this.pool = pool;
			this.sleepTime = sleepTime;
			if (sleepTime <= 0) {
				log.warn("Database connection pool maintenance thread interval is set to 0, defaulting to 30 seconds");
				this.sleepTime = 1000 * 30;
			} else if (sleepTime < 1000) {
				log.warn("Database connection pool maintenance thread interval is set to lower than 1 second.");
			}
		}

		@Override
		public void run() {
			while (run) {
				try {
					sleep(sleepTime);
				} catch (InterruptedException e) {
					// ignore it
					Thread.interrupted();
					continue;
				} // catch

				if (pool.isClosed()) {
					if (pool.getSize() <= 0) {
						run = false;
					}
				} else {
					try {
						if (pool.getPoolProperties().isRemoveAbandoned())
							pool.checkAbandoned();
						if (pool.getPoolProperties().getMinIdle() < pool.idle.size())
							pool.checkIdle();
						if (pool.getPoolProperties().isTestWhileIdle())
							pool.testAllIdle();
						if (pool.getPoolProperties().isAutomaticHostDiscovery()) {
							pool.refreshRing();
						}
					} catch (Exception x) {
						log.error("", x);
					} // catch
				} // end if
			} // while
		} // run

		public void stopRunning() {
			run = false;
			interrupt();
		}
	}
}
