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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a pooled connection
 * and holds a reference to the {@link org.apache.cassandra.thrift.Cassandra.Client} and {@link org.apache.thrift.transport.TTransport} object
 * 
 * Derived from org.apache.tomcat.jdbc.pool.PooledConnection by fhanik
 * 
 * @version 1.0
 */
public class PooledConnection {
    /**
     * Logger
     */
    private static final Logger log = LoggerFactory.getLogger(PooledConnection.class);
    /**
     * Instance counter
     */
    protected static AtomicInteger counter = new AtomicInteger(01);

    /**
     * Validate when connection is borrowed flag
     */
    public static final int VALIDATE_BORROW = 1;
    /**
     * Validate when connection is returned flag
     */
    public static final int VALIDATE_RETURN = 2;
    /**
     * Validate when connection is idle flag
     */
    public static final int VALIDATE_IDLE = 3;
    /**
     * Validate when connection is initialized flag
     */
    public static final int VALIDATE_INIT = 4;

    /**
     * The properties for the connection pool
     */
    protected PoolConfiguration poolProperties;
    /**
     * The underlying database connection
     */
    private volatile Cassandra.Client connection;
    
    /**
     * The underlying transport for the connection
     */
    private volatile TTransport transport;
    /**
     * When we track abandon traces, this string holds the thread dump
     */
    private String abandonTrace = null;
    /**
     * Timestamp the connection was last 'touched' by the pool
     */
    private volatile long timestamp;
    /**
     * Lock for this connection only
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(false);
    /**
     * Set to true if this connection has been discarded by the pool
     */
    private volatile boolean discarded = false;
    /**
     * The Timestamp when the last time the connect() method was called successfully
     */
    private volatile long lastConnected = -1;
    /**
     * timestamp to keep track of validation intervals
     */
    private volatile long lastValidated = System.currentTimeMillis();
    /**
     * The instance number for this connection
     */
    private int instanceCount = 0;
    /**
     * The parent
     */
    protected ConnectionPool parent;
    
    private HashMap<Object, Object> attributes = new HashMap<Object, Object>();
    
    private AtomicBoolean released = new AtomicBoolean(false);
    
    private volatile boolean suspect = false;
    
    /**
     * Constructor
     * @param prop - pool properties
     * @param parent - the parent connection pool
     */
    public PooledConnection(PoolConfiguration prop, ConnectionPool parent) {
        instanceCount = counter.addAndGet(1);
        poolProperties = prop;
        this.parent = parent;
    }

    public void connect() throws TException {
        if (released.get()) throw new TException("A connection once released, can't be reestablished.");
        if (connection != null) {
            try {
                this.disconnect(false);
            } catch (Exception x) {
                log.debug("Unable to disconnect previous connection.", x);
            } //catch
        } //end if
        
        List<CassandraHost> hosts = parent.getCassandraRing().getHosts();
        Iterator<CassandraHost> hostIterator = hosts.iterator();
        int tried = 0;
        for(this.transport=null; this.transport==null; ) {
        	if(tried>poolProperties.getFailoverPolicy().numRetries || !hostIterator.hasNext()) {
        		throw new TException("Could not connect to any hosts");
        	}
        	CassandraHost host = hostIterator.next();
        	// If the host is good or the validation interval has passed since last checking with it, attempt to get a connection
        	if(host.isGood() || (host.getLastUsed()+poolProperties.getHostRetryInterval() < System.currentTimeMillis())) {        		
		        try {
			        TSocket socket = new TSocket(host.getHost(), poolProperties.getPort(), poolProperties.getSocketTimeout());	    
					if (poolProperties.isFramed())
						this.transport = new TFramedTransport(socket);
					else
						this.transport = socket;
					host.timestamp();
					this.transport.open();
					host.setGood(true);
		        } catch (TTransportException tte) {
		        	host.timestamp();
		        	host.setGood(false);
		        	log.warn("Failed connection to "+host);		        	
		        	this.transport = null;
		        	tried++;
		        }
        	}
        }
		TProtocol protocol = new TBinaryProtocol(this.transport);

		this.connection = new Cassandra.Client(protocol);
                        
        this.discarded = false;
        this.lastConnected = System.currentTimeMillis();        
    }
    
    
    /**
     * 
     * @return true if connect() was called successfully and disconnect has not yet been called
     */
    public boolean isInitialized() {
        return connection!=null;
    }

    /**
     * Issues a call to {@link #disconnect(boolean)} with the argument false followed by a call to 
     * {@link #connect()}
     * @throws SQLException if the call to {@link #connect()} fails.
     */
    public void reconnect() throws TException {
        this.disconnect(false);
        this.connect();
    } //reconnect

    /**
     * Disconnects the connection. All exceptions are logged using debug level.
     * @param finalize if set to true, a call to {@link ConnectionPool#finalize(PooledConnection)} is called.
     */
    private void disconnect(boolean finalize) {
        if (isDiscarded()) {
            return;
        }
        setDiscarded(true);
        if (connection != null) {
            try {
                parent.disconnectEvent(this, finalize);
                transport.close();
            }catch (Exception ignore) {
                if (log.isDebugEnabled()) {
                    log.debug("Unable to close underlying SQL connection",ignore);
                }
            }
        }
        connection = null;
        transport = null;
        lastConnected = -1;
        if (finalize) parent.finalize(this);
    }


//============================================================================
//             
//============================================================================

    /**
     * Returns abandon timeout in milliseconds
     * @return abandon timeout in milliseconds
     */
    public long getAbandonTimeout() {
        if (poolProperties.getRemoveAbandonedTimeout() <= 0) {
            return Long.MAX_VALUE;
        } else {
            return poolProperties.getRemoveAbandonedTimeout()*1000;
        } //end if
    }

    /**
     * Returns true if the connection pool is configured 
     * to do validation for a certain action.
     * @param action
     * @return
     */
    private boolean doValidate(int action) {
        if (action == PooledConnection.VALIDATE_BORROW &&
            poolProperties.isTestOnBorrow())
            return true;
        else if (action == PooledConnection.VALIDATE_RETURN &&
                 poolProperties.isTestOnReturn())
            return true;
        else if (action == PooledConnection.VALIDATE_IDLE &&
                 poolProperties.isTestWhileIdle())
            return true;
        else if (action == PooledConnection.VALIDATE_INIT &&
                 poolProperties.isTestOnConnect())
            return true;        
        else
            return false;
    }

    /**
     * Returns true if the object is still valid. if not
     * the pool will call the getExpiredAction() and follow up with one
     * of the four expired methods
     */
    public boolean validate(int validateAction) {
        if (this.isDiscarded()) {
            return false;
        }
        
        if (!doValidate(validateAction)) {
            //no validation required, no init sql and props not set
            return true;
        }

        //Don't bother validating if already have recently enough
        long now = System.currentTimeMillis();
        if (validateAction!=VALIDATE_INIT &&
            poolProperties.getValidationInterval() > 0 &&
            (now - this.lastValidated) <
            poolProperties.getValidationInterval()) {
            return true;
        }

        try {
        	parent.getCassandraRing().refresh(connection); // Bonus: we validate the connection and also get an update list of hosts from Cassandra
            this.lastValidated = now;
            return true;
        } catch (Exception ignore) {
            if (log.isDebugEnabled())
                log.debug("Unable to validate object:",ignore);
        }
        return false;
    } //validate

    /**
     * The time limit for how long the object
     * can remain unused before it is released
     * @return {@link PoolConfiguration#getMinEvictableIdleTimeMillis()}
     */
    public long getReleaseTime() {
        return this.poolProperties.getMinEvictableIdleTimeMillis();
    }

    /**
     * This method is called if (Now - timeCheckedIn > getReleaseTime())
     * This method disconnects the connection, logs an error in debug mode if it happens
     * then sets the {@link #released} flag to false. Any attempts to connect this cached object again
     * will fail per {@link #connect()}
     * The connection pool uses the atomic return value to decrement the pool size counter.
     * @return true if this is the first time this method has been called. false if this method has been called before.
     */
    public boolean release() {
        try {
            disconnect(true);
        } catch (Exception x) {
            if (log.isDebugEnabled()) {
                log.debug("Unable to close SQL connection",x);
            }
        }
        return released.compareAndSet(false, true);

    }

    /**
     * The pool will set the stack trace when it is check out and
     * checked in
     * @param trace the stack trace for this connection
     */

    public void setStackTrace(String trace) {
        abandonTrace = trace;
    }

    /**
     * Returns the stack trace from when this connection was borrowed. Can return null if no stack trace was set.
     * @return the stack trace or null of no trace was set
     */
    public String getStackTrace() {
        return abandonTrace;
    }

    /**
     * Sets a timestamp on this connection. A timestamp usually means that some operation
     * performed successfully.
     * @param timestamp the timestamp as defined by {@link System#currentTimeMillis()}
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        setSuspect(false);
    }


    public boolean isSuspect() {
        return suspect;
    }

    public void setSuspect(boolean suspect) {
        this.suspect = suspect;
    }

    /**
     * An interceptor can call this method with the value true, and the connection will be closed when it is returned to the pool.
     * @param discarded - only valid value is true
     * @throws IllegalStateException if this method is called with the value false and the value true has already been set.
     */
    public void setDiscarded(boolean discarded) {
        if (this.discarded && !discarded) throw new IllegalStateException("Unable to change the state once the connection has been discarded");
        this.discarded = discarded;
    }

    /**
     * Set the timestamp the connection was last validated.
     * This flag is used to keep track when we are using a {@link PoolConfiguration#setValidationInterval(long) validation-interval}.
     * @param lastValidated a timestamp as defined by {@link System#currentTimeMillis()} 
     */
    public void setLastValidated(long lastValidated) {
        this.lastValidated = lastValidated;
    }

    /**
     * Sets the pool configuration for this connection and connection pool.
     * Object is shared with the {@link ConnectionPool}
     * @param poolProperties
     */
    public void setPoolProperties(PoolConfiguration poolProperties) {
        this.poolProperties = poolProperties;
    }

    /**
     * Return the timestamps of last pool action. Timestamps are typically set when connections 
     * are borrowed from the pool. It is used to keep track of {@link PoolConfiguration#setRemoveAbandonedTimeout(int) abandon-timeouts}.
     *    
     * @return the timestamp of the last pool action as defined by {@link System#currentTimeMillis()}
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns the discarded flag.
     * @return the discarded flag. If the value is true, 
     * either {@link #disconnect(boolean)} has been called or it will be called when the connection is returned to the pool.
     */
    public boolean isDiscarded() {
        return discarded;
    }

    /**
     * Returns the timestamp of the last successful validation query execution. 
     * @return the timestamp of the last successful validation query execution as defined by {@link System#currentTimeMillis()}
     */
    public long getLastValidated() {
        return lastValidated;
    }

    /**
     * Returns the configuration for this connection and pool
     * @return the configuration for this connection and pool
     */
    public PoolConfiguration getPoolProperties() {
        return poolProperties;
    }

    /**
     * Locks the connection only if either {@link PoolConfiguration#isPoolSweeperEnabled()} or 
     * {@link PoolConfiguration#getUseLock()} return true. The per connection lock ensures thread safety is
     * multiple threads are performing operations on the connection. 
     * Otherwise this is a noop for performance
     */
    public void lock() {
        if (poolProperties.getUseLock() || this.poolProperties.isPoolSweeperEnabled()) {
            //optimized, only use a lock when there is concurrency
            lock.writeLock().lock();
        }
    }

    /**
     * Unlocks the connection only if the sweeper is enabled
     * Otherwise this is a noop for performance
     */
    public void unlock() {
        if (poolProperties.getUseLock() || this.poolProperties.isPoolSweeperEnabled()) {
          //optimized, only use a lock when there is concurrency
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the underlying connection
     * @return the underlying JDBC connection as it was returned from the JDBC driver
     * @see javax.sql.PooledConnection#getConnection()
     */
    public Cassandra.Client getConnection() {
        return this.connection;
    }
        
    public TTransport getTransport() {
        return this.transport;
    }
    
    
    /**
     * Returns the timestamp of when the connection was last connected to the database.
     * ie, a successful call to {@link java.sql.Driver#connect(String, java.util.Properties)}.
     * @return the timestamp when this connection was created as defined by {@link System#currentTimeMillis()}
     */
    public long getLastConnected() {
        return lastConnected;
    }
    
    @Override
    public String toString() {
        return "PooledConnection[instance="+instanceCount+","+(connection!=null?connection.toString():"null")+"]";
    }
    
    /**
     * Returns true if this connection has been released and wont be reused.
     * @return true if the method {@link #release()} has been called
     */
    public boolean isReleased() {
        return released.get();
    }
    
    public HashMap<Object,Object> getAttributes() {
        return attributes;
    }
}
