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


/**
 * A list of properties that are configurable for a connection pool.
 * 
 * @author Tristan Tarrant
 *
 */

public interface PoolConfiguration {
	
	/**
	 * Provides a generic setter for any of the declared properties of the PoolConfiguration
	 * 
	 * @param name the name of the property
	 * @param value the new value of the property
	 */
	public void set(String name, Object value);
	
	/**
	 * Provides a generic getter for any of the declared properties of the PoolConfiguration
	 * 
	 * @param name the name of the property
	 * @return the value of the property
	 */
	public Object get(String name);

	/**
	 * Sets the Cassandra host. May be a comma-separated list of addresses
	 * 
	 * @param host
	 */
    public void setHost(String host);
    
    /**
     * Returns the Cassandra hosts
     * 
     * @return a single string representing the Cassandra host addresses separated by commas
     */
    public String getHost();
    
    /**
     * Sets the Cassandra connection URL.
     * The url is in the form cassandra:thrift://[host]:[port]
     * 
     * @param url
     */
    public void setUrl(String url);
    
    /**
     * Returns the Cassandra connection URL
     * 
     * @return
     */
    public String getUrl();
    
    /**
     * Sets the Cassandra port
     * 
     * @param port
     */
    public void setPort(int port);
    
    /**
     * Returns the Cassandra port (defaults to 9160)
     * 
     * @return the Cassandra port
     */
    public int getPort();
    
    /**
     * Sets whether to use framed connection mode (default false)
     * @param framed
     */
    public void setFramed(boolean framed);
    
    /**
     * Returns whether framed connection mode is being used
     * @return whether framed connection mode is being used
     */
    public boolean isFramed();
    
    /**
     * Sets whether Cassandra hosts should be queried to automatically obtain a list of other hosts
     * @param autoDiscovery
     */
    public void setAutomaticHostDiscovery(boolean autoDiscovery);
    
    /**
     * Returns whether automatic host discovery is being used
     * 
     * @return whether automatic host discovery is being used
     */
    public boolean isAutomaticHostDiscovery();
    
    /**
     * Sets the host failover policy, i.e. what to do when connecting to a host fails
     */
    public void setFailoverPolicy(HostFailoverPolicy failoverPolicy);
    
    /**
     * Returns the failover policy.
     * 
     * @return the failover policy
     */
    public HostFailoverPolicy getFailoverPolicy();

    /**
     * Connections that have been abandoned (timed out) wont get closed and reported up unless the number of connections in use are 
     * above the percentage defined by abandonWhenPercentageFull. 
     * The value should be between 0-100. 
     * The default value is 0, which implies that connections are eligible for 
     * closure as soon as removeAbandonedTimeout has been reached.
     * @param percentage a value between 0 and 100 to indicate when connections that have been abandoned/timed out are considered abandoned
     */
    public void setAbandonWhenPercentageFull(int percentage);

    /**
     * Connections that have been abandoned (timed out) wont get closed and reported up unless the number of connections in use are 
     * above the percentage defined by abandonWhenPercentageFull. 
     * The value should be between 0-100. 
     * The default value is 0, which implies that connections are eligible for 
     * closure as soon as removeAbandonedTimeout has been reached.
     * @return percentage - a value between 0 and 100 to indicate when connections that have been abandoned/timed out are considered abandoned
     */
    public int getAbandonWhenPercentageFull();

    /**
     * Returns true if a fair queue is being used by the connection pool
     * @return true if a fair waiting queue is being used
     */
    public boolean isFairQueue();

    /**
     * Set to true if you wish that calls to getConnection 
     * should be treated fairly in a true FIFO fashion. 
     * This uses the {@link FairBlockingQueue} implementation for the list of the idle connections. 
     * The default value is true. 
     * This flag is required when you want to use asynchronous connection retrieval.
     * @param fairQueue
     */
    public void setFairQueue(boolean fairQueue);
    
    /**
     * Returns the number of connections that will be established when the connection pool is started.
     * Default value is 10 
     * @return number of connections to be started when pool is started
     */
    public int getInitialSize();
    
    /**
     * Set the number of connections that will be established when the connection pool is started.
     * Default value is 10.
     * If this value exceeds {@link #setMaxActive(int)} it will automatically be lowered. 
     * @param initialSize the number of connections to be established.
     * 
     */
    public void setInitialSize(int initialSize);

    /**
     * boolean flag to set if stack traces should be logged for application code which abandoned a Connection. 
     * Logging of abandoned Connections adds overhead for every Connection borrow because a stack trace has to be generated. 
     * The default value is false.
     * @return true if the connection pool logs stack traces when connections are borrowed from the pool.
     */
    public boolean isLogAbandoned();
    
    /**
     * boolean flag to set if stack traces should be logged for application code which abandoned a Connection. 
     * Logging of abandoned Connections adds overhead for every Connection borrow because a stack trace has to be generated. 
     * The default value is false.
     * @param logAbandoned set to true if stack traces should be recorded when {@link DataSource#getConnection()} is called.
     */
    public void setLogAbandoned(boolean logAbandoned);

    /**
     * The maximum number of active connections that can be allocated from this pool at the same time. The default value is 100
     * @return the maximum number of connections used by this pool
     */
    public int getMaxActive();
    
    /**
     * The maximum number of active connections that can be allocated from this pool at the same time. The default value is 100
     * @param maxActive hard limit for number of managed connections by this pool
     */
    public void setMaxActive(int maxActive);

    
    /**
     * The maximum number of connections that should be kept in the idle pool if {@link #isPoolSweeperEnabled()} returns false.
     * If the If {@link #isPoolSweeperEnabled()} returns true, then the idle pool can grow up to {@link #getMaxActive}
     * and will be shrunk according to {@link #getMinEvictableIdleTimeMillis()} setting.
     * Default value is maxActive:100  
     * @return the maximum number of idle connections.
     */
    public int getMaxIdle();
    
    /**
     * The maximum number of connections that should be kept in the idle pool if {@link #isPoolSweeperEnabled()} returns false.
     * If the If {@link #isPoolSweeperEnabled()} returns true, then the idle pool can grow up to {@link #getMaxActive}
     * and will be shrunk according to {@link #getMinEvictableIdleTimeMillis()} setting.
     * Default value is maxActive:100  
     * @param maxIdle the maximum size of the idle pool
     */
    public void setMaxIdle(int maxIdle);

    /**
     * The maximum number of milliseconds that the pool will wait (when there are no available connections and the 
     * {@link #getMaxActive} has been reached) for a connection to be returned 
     * before throwing an exception. Default value is 30000 (30 seconds)
     * @return the number of milliseconds to wait for a connection to become available if the pool is maxed out.
     */
    public int getMaxWait();

    /**
     * The maximum number of milliseconds that the pool will wait (when there are no available connections and the 
     * {@link #getMaxActive} has been reached) for a connection to be returned 
     * before throwing an exception. Default value is 30000 (30 seconds)
     * @param maxWait the maximum number of milliseconds to wait.
     */
    public void setMaxWait(int maxWait);

    /**
     * The minimum amount of time an object must sit idle in the pool before it is eligible for eviction. 
     * The default value is 60000 (60 seconds).
     * @return the minimum amount of idle time in milliseconds before a connection is considered idle and eligible for eviction. 
     */
    public int getMinEvictableIdleTimeMillis();
    
    /**
     * The minimum amount of time an object must sit idle in the pool before it is eligible for eviction. 
     * The default value is 60000 (60 seconds).
     * @param minEvictableIdleTimeMillis the number of milliseconds a connection must be idle to be eligible for eviction.
     */
    public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis);

    /**
     * The minimum number of established connections that should be kept in the pool at all times. 
     * The connection pool can shrink below this number if validation queries fail and connections get closed. 
     * Default value is derived from {@link #getInitialSize()} (also see {@link #setTestWhileIdle(boolean)}
     * The idle pool will not shrink below this value during an eviction run, hence the number of actual connections
     * can be between {@link #getMinIdle()} and somewhere between {@link #getMaxIdle()} and {@link #getMaxActive()}
     * @return the minimum number of idle or established connections
     */
    public int getMinIdle();
    
    /**
     * The minimum number of established connections that should be kept in the pool at all times. 
     * The connection pool can shrink below this number if validation queries fail and connections get closed. 
     * Default value is derived from {@link #getInitialSize()} (also see {@link #setTestWhileIdle(boolean)}
     * The idle pool will not shrink below this value during an eviction run, hence the number of actual connections
     * can be between {@link #getMinIdle()} and somewhere between {@link #getMaxIdle()} and {@link #getMaxActive()}
     * 
     * @param minIdle the minimum number of idle or established connections
     */
    public void setMinIdle(int minIdle);

    /**
     * Returns the name of the connection pool. By default a JVM unique random name is assigned.
     * @return the name of the pool, should be unique in a JVM
     */
    public String getName();
    
    /**
     * Sets the name of the connection pool 
     * @param name the name of the pool, should be unique in a runtime JVM
     */
    public void setName(String name);

    /**
     * Property not used
     * @return unknown value
     */
    public int getNumTestsPerEvictionRun();
    
    /**
     * Property not used
     * @param numTestsPerEvictionRun parameter ignored.
     */
    public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun);

    /**
     * Returns the password used when establishing connections to the database.
     * @return the password in string format
     */
    public String getPassword();
    
    /**
     * Sets the password to establish the connection with.
     * 
     * @param password 
     */
    public void setPassword(String password);

    /**
     * @see #getName()
     * @return name
     */
    public String getPoolName();
    
    /**
     * Returns the username used to establish the connection with
     * @return the username used to establish the connection with
     */
    public String getUsername();

    /**
     * Sets the username used to establish the connection with
     * 
     * @param username 
     */
    public void setUsername(String username);


    /**
     * boolean flag to remove abandoned connections if they exceed the removeAbandonedTimout. 
     * If set to true a connection is considered abandoned and eligible for removal if it has 
     * been in use longer than the {@link #getRemoveAbandonedTimeout()} and the condition for 
     * {@link #getAbandonWhenPercentageFull()} is met.
     * Setting this to true can recover db connections from applications that fail to close a connection. 
     * See also {@link #isLogAbandoned()} The default value is false.
     * @return true if abandoned connections can be closed and expelled out of the pool
     */
    public boolean isRemoveAbandoned();
    
    /**
     * boolean flag to remove abandoned connections if they exceed the removeAbandonedTimout. 
     * If set to true a connection is considered abandoned and eligible for removal if it has 
     * been in use longer than the {@link #getRemoveAbandonedTimeout()} and the condition for 
     * {@link #getAbandonWhenPercentageFull()} is met.
     * Setting this to true can recover db connections from applications that fail to close a connection. 
     * See also {@link #isLogAbandoned()} The default value is false.
     * @param removeAbandoned set to true if abandoned connections can be closed and expelled out of the pool
     */
    public void setRemoveAbandoned(boolean removeAbandoned);

    /**
     * The time in seconds before a connection can be considered abandoned.
     * The timer can be reset upon queries using an interceptor.
     * @param removeAbandonedTimeout the time in seconds before a used connection can be considered abandoned     * 
     */
    public void setRemoveAbandonedTimeout(int removeAbandonedTimeout);

    /**
     * The time in seconds before a connection can be considered abandoned.
     * The timer can be reset upon queries using an interceptor. 
     * @return the time in seconds before a used connection can be considered abandoned
     */ 
    public int getRemoveAbandonedTimeout();

    /**
     * The indication of whether objects will be validated before being borrowed from the pool. 
     * If the object fails to validate, it will be dropped from the pool, and we will attempt to borrow another. 
     * NOTE - for a true value to have any effect, the validationQuery parameter must be set to a non-null string. 
     * Default value is false
     * In order to have a more efficient validation, see {@link #setValidationInterval(long)}
     * @return true if the connection is to be validated upon borrowing a connection from the pool
     * @see #getValidationInterval()
     */
    public boolean isTestOnBorrow();
    
    /**
     * The indication of whether objects will be validated before being borrowed from the pool. 
     * If the object fails to validate, it will be dropped from the pool, and we will attempt to borrow another. 
     * NOTE - for a true value to have any effect, the validationQuery parameter must be set to a non-null string. 
     * Default value is false
     * In order to have a more efficient validation, see {@link #setValidationInterval(long)}
     * @param testOnBorrow set to true if validation should take place before a connection is handed out to the application
     * @see #getValidationInterval()
     */
    public void setTestOnBorrow(boolean testOnBorrow);
    
    /**
     * The indication of whether objects will be validated after being returned to the pool. 
     * If the object fails to validate, it will be dropped from the pool. 
     * NOTE - for a true value to have any effect, the validationQuery parameter must be set to a non-null string. 
     * Default value is false
     * In order to have a more efficient validation, see {@link #setValidationInterval(long)}
     * @return true if validation should take place after a connection is returned to the pool
     * @see #getValidationInterval()
     */
    public boolean isTestOnReturn();

    /**
     * The indication of whether objects will be validated after being returned to the pool. 
     * If the object fails to validate, it will be dropped from the pool. 
     * NOTE - for a true value to have any effect, the validationQuery parameter must be set to a non-null string. 
     * Default value is false
     * In order to have a more efficient validation, see {@link #setValidationInterval(long)}
     * @param testOnReturn true if validation should take place after a connection is returned to the pool
     * @see #getValidationInterval()
     */
    public void setTestOnReturn(boolean testOnReturn);
    
    
    /**
     * Set to true if query validation should take place while the connection is idle.
     * @return true if validation should take place during idle checks
     * @see #setTimeBetweenEvictionRunsMillis(int)
     */
    public boolean isTestWhileIdle();
    
    /**
     * Set to true if query validation should take place while the connection is idle.
     * @param testWhileIdle true if validation should take place during idle checks
     * @see #setTimeBetweenEvictionRunsMillis(int)
     */
    public void setTestWhileIdle(boolean testWhileIdle);
    
    /**
     * The number of milliseconds to sleep between runs of the idle connection validation, abandoned cleaner 
     * and idle pool resizing. This value should not be set under 1 second. 
     * It dictates how often we check for idle, abandoned connections, and how often we validate idle connection and resize the idle pool.
     * The default value is 5000 (5 seconds)
     * @return the sleep time in between validations in milliseconds
     */
    public int getTimeBetweenEvictionRunsMillis();
    
    /**
     * The number of milliseconds to sleep between runs of the idle connection validation, abandoned cleaner 
     * and idle pool resizing. This value should not be set under 1 second. 
     * It dictates how often we check for idle, abandoned connections, and how often we validate idle connection and resize the idle pool.
     * The default value is 5000 (5 seconds)
     * @param timeBetweenEvictionRunsMillis the sleep time in between validations in milliseconds
     */
    public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis);
        
    /**
     * avoid excess validation, only run validation at most at this frequency - time in milliseconds. 
     * If a connection is due for validation, but has been validated previously 
     * within this interval, it will not be validated again. 
     * The default value is 30000 (30 seconds).
     * @return the validation interval in milliseconds
     */
    public long getValidationInterval();
    
    /**
     * avoid excess validation, only run validation at most at this frequency - time in milliseconds. 
     * If a connection is due for validation, but has been validated previously 
     * within this interval, it will not be validated again. 
     * The default value is 30000 (30 seconds).
     * @param validationInterval the validation interval in milliseconds
     */
    public void setValidationInterval(long validationInterval);
    
    /**
     * Returns true if we should run the validation query when connecting to the database for the first time on a connection.
     * Normally this is always set to false.
     * @return true if we should run the validation query upon connect
     */
    public boolean isTestOnConnect();

    /**
     * Set to true if we should run the validation query when connecting to the database for the first time on a connection.
     * Normally this is always set to false.
     * 
     * @param testOnConnect set to true if we should run the validation query upon connect
     */
    public void setTestOnConnect(boolean testOnConnect);
    
    /**
     * If set to true, the connection pool creates a {@link net.dataforte.cassandra.pool.jmx.ConnectionPoolMBean} object 
     * that can be registered with JMX to receive notifications and state about the pool.
     * The ConnectionPool object doesn't register itself, as there is no way to keep a static non changing ObjectName across JVM restarts.
     * @return true if the mbean object will be created upon startup.
     */
    public boolean isJmxEnabled();

    /**
     * If set to true, the connection pool creates a {@link net.dataforte.cassandra.pool.jmx.ConnectionPoolMBean} object 
     * that can be registered with JMX to receive notifications and state about the pool.
     * The ConnectionPool object doesn't register itself, as there is no way to keep a static non changing ObjectName across JVM restarts.
     * @param jmxEnabled set to to if the mbean object should be created upon startup.
     */
    public void setJmxEnabled(boolean jmxEnabled);

    /**
     * Returns true if the pool sweeper is enabled for the connection pool.
     * The pool sweeper is enabled if any settings that require async intervention in the pool are turned on
     * <source>
        boolean result = getTimeBetweenEvictionRunsMillis()>0;
        result = result && (isRemoveAbandoned() && getRemoveAbandonedTimeout()>0);
        result = result || (isTestWhileIdle() && getValidationQuery()!=null);
        return result;
       </source> 
     *
     * @return true if a background thread is or will be enabled for this pool
     */
    public boolean isPoolSweeperEnabled();

    /**
     * Set to true if you wish the <code>ProxyConnection</code> class to use <code>String.equals</code> instead of 
     * <code>==</code> when comparing method names. 
     * This property does not apply to added interceptors as those are configured individually.
     * The default value is <code>false</code>.
     * @return true if pool uses {@link String#equals(Object)} instead of == when comparing method names on {@link java.sql.Connection} methods
     */
    public boolean isUseEquals();

    /**
     * Set to true if you wish the <code>ProxyConnection</code> class to use <code>String.equals</code> instead of 
     * <code>==</code> when comparing method names. 
     * This property does not apply to added interceptors as those are configured individually.
     * The default value is <code>false</code>.
     * @param useEquals set to true if the pool should use {@link String#equals(Object)} instead of ==
     * when comparing method names on {@link java.sql.Connection} methods
     */
    public void setUseEquals(boolean useEquals);

    /**
     * Time in milliseconds to keep this connection alive even when used. 
     * When a connection is returned to the pool, the pool will check to see if the 
     * ((now - time-when-connected) > maxAge) has been reached, and if so, 
     * it closes the connection rather than returning it to the pool. 
     * The default value is 0, which implies that connections will be left open and no 
     * age check will be done upon returning the connection to the pool.
     * This is a useful setting for database sessions that leak memory as it ensures that the session
     * will have a finite life span.
     * @return the time in milliseconds a connection will be open for when used
     */
    public long getMaxAge();

    /**
     * Time in milliseconds to keep this connection alive even when used. 
     * When a connection is returned to the pool, the pool will check to see if the 
     * ((now - time-when-connected) > maxAge) has been reached, and if so, 
     * it closes the connection rather than returning it to the pool. 
     * The default value is 0, which implies that connections will be left open and no 
     * age check will be done upon returning the connection to the pool.
     * This is a useful setting for database sessions that leak memory as it ensures that the session
     * will have a finite life span.
     * @param maxAge the time in milliseconds a connection will be open for when used
     */
    public void setMaxAge(long maxAge);

    /**
     * Return true if a lock should be used when operations are performed on the connection object.
     * Should be set to false unless you plan to have a background thread of your own doing idle and abandon checking
     * such as JMX clients. If the pool sweeper is enabled, then the lock will automatically be used regardless of this setting.
     * @return true if a lock is used.
     */
    public boolean getUseLock();

    /**
     * Set to true if a lock should be used when operations are performed on the connection object.
     * Should be set to false unless you plan to have a background thread of your own doing idle and abandon checking
     * such as JMX clients. If the pool sweeper is enabled, then the lock will automatically be used regardless of this setting.
     * @param useLock set to true if a lock should be used on connection operations
     */
    public void setUseLock(boolean useLock);
    
    /**
     * Similar to {@link #setRemoveAbandonedTimeout(int)} but instead of treating the connection
     * as abandoned, and potentially closing the connection, this simply logs the warning if 
     * {@link #isLogAbandoned()} returns true. If this value is equal or less than 0, no suspect 
     * checking will be performed. Suspect checking only takes place if the timeout value is larger than 0 and
     * the connection was not abandoned or if abandon check is disabled. If a connection is suspect a WARN message gets
     * logged and a JMX notification gets sent once. 
     * @param seconds - the amount of time in seconds that has to pass before a connection is marked suspect. 
     */
    public void setSuspectTimeout(int seconds);
    
    /**
     * Returns the time in seconds to pass before a connection is marked an abanoned suspect.
     * Any value lesser than or equal to 0 means the check is disabled. 
     * @return Returns the time in seconds to pass before a connection is marked an abanoned suspect.
     */
    public int getSuspectTimeout();

    /**
     * Returns the socket timeout in milliseconds
     * 
     * @return the socket timeout in milliseconds
     */
	int getSocketTimeout();

	/**
	 * Sets the socket timeout in milliseconds
	 * 
	 * @param socketTimeout
	 */
	void setSocketTimeout(int socketTimeout);

	/**
	 * Returns an array of configured hosts (may be different from the actual list if dynamic discovery is enabled)
	 * 
	 * @return an array of strings representing the addresses of the configured Cassandra hosts
	 */
	String[] getConfiguredHosts();

	/**
	 * Returns the interval in milliseconds before retrying a host to which a connection has failed in the past.
	 * Default is 300000 (5 minutes)
	 * 
	 * @return milliseconds before host retry
	 */
	public long getHostRetryInterval();
	
	/**
	 * Sets the interval in milliseconds before retrying a host to which a connection has failed in the past.
	 * @param hostRetryInterval number of millieseconds before retrying a host
	 */
	void setHostRetryInterval(long hostRetryInterval);
	
	/**
     * Configure the connection pool to use a DataSource according to {@link PoolConfiguration#setDataSource(Object)}
     * But instead of injecting the object, specify the JNDI location.
     * After a successful JNDI look, the {@link PoolConfiguration#getDataSource()} will not return null. 
     * @param jndiDS -the JNDI string @TODO specify the rules here.
     */
    public void setDataSourceJNDI(String jndiDS);
    
    /**
     * Returns the JNDI string configured for data source usage.
     * @return the JNDI string or null if not set
     */
    public String getDataSourceJNDI();

    /**
     * Injects a datasource that will be used to retrieve/create connections.
     * 
     * @param ds the {@link javax.sql.DataSource} to be used for creating connections to be pooled.
     */
    public void setDataSource(Object ds);
    
    /**
     * Returns a datasource, if one exists that is being used to create connections.
     * 
     * @return the datasource object
     */
    public Object getDataSource();
}
