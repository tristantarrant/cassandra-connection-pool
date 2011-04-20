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

import java.lang.management.ManagementFactory;
import java.util.Hashtable;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistration;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import net.dataforte.cassandra.thrift.CassandraThriftDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DataSource that can be instantiated through IoC and implements the
 * CassandraThriftDataSource interface since the DataSourceProxy is used as a generic proxy.
 * The DataSource simply wraps a {@link ConnectionPool} in order to provide a
 * standard interface to the user.
 * 
 * @author Tristan Tarrant
 */
public class DataSource extends DataSourceProxy implements PoolConfiguration, CassandraThriftDataSource, MBeanRegistration {
	private static final Logger log = LoggerFactory.getLogger(DataSource.class);

	/**
	 * Constructor for reflection only. A default set of pool properties will be
	 * created.
	 */
	public DataSource() {
		super(new PoolProperties());
	}

	/**
	 * Constructs a DataSource object wrapping a connection
	 * 
	 * @param poolProperties
	 */
	public DataSource(PoolConfiguration poolProperties) {
		super(poolProperties);
	}

	// ===============================================================================
	// JMX Operations - Register the actual pool itself under the
	// cassandra.thrift domain
	// ===============================================================================
	protected volatile ObjectName oname = null;

	/**
	 * Unregisters the underlying connection pool mbean.<br/> {@inheritDoc}
	 */
	public void postDeregister() {
		if (oname != null)
			unregisterJmx();
	}

	/**
	 * no-op<br/> {@inheritDoc}
	 */
	public void postRegister(Boolean registrationDone) {
		// NOOP
	}

	/**
	 * no-op<br/> {@inheritDoc}
	 */
	public void preDeregister() throws Exception {
		// NOOP
	}

	/**
	 * If the connection pool MBean exists, it will be registered during this
	 * operation.<br/> {@inheritDoc}
	 */
	public ObjectName preRegister(MBeanServer server, ObjectName name) throws Exception {
		try {
			this.oname = createObjectName(name);
			if (oname != null)
				registerJmx();
		} catch (MalformedObjectNameException x) {
			log.error("Unable to create object name for connection pool.", x);
		}
		return name;
	}

	/**
	 * Creates the ObjectName for the ConnectionPoolMBean object to be
	 * registered
	 * 
	 * @param original
	 *            the ObjectName for the DataSource
	 * @return the ObjectName for the ConnectionPoolMBean
	 * @throws MalformedObjectNameException
	 */
	public ObjectName createObjectName(ObjectName original) throws MalformedObjectNameException {
		String domain = ConnectionPool.POOL_JMX_PREFIX;
		Hashtable<String, String> properties = original.getKeyPropertyList();
		String origDomain = original.getDomain();
		properties.put("type", "ConnectionPool");
		properties.put("class", this.getClass().getName());
		if (original.getKeyProperty("path") != null) {
			properties.put("engine", origDomain);
		}
		ObjectName name = new ObjectName(domain, properties);
		return name;
	}

	/**
	 * Registers the ConnectionPoolMBean under a unique name based on the
	 * ObjectName for the DataSource
	 */
	protected void registerJmx() {
		try {
			if (pool.getJmxPool() != null) {
				MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
				mbs.registerMBean(pool.getJmxPool(), oname);
			}
		} catch (Exception e) {
			log.error("Unable to register connection pool with JMX", e);
		}
	}

	/**
     * 
     */
	protected void unregisterJmx() {
		try {
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			mbs.unregisterMBean(oname);
		} catch (InstanceNotFoundException ignore) {
			// NOOP
		} catch (Exception e) {
			log.error("Unable to unregister connection pool with JMX", e);
		}
	}
	
	/**************************************************************************************
	 * Properties
	 **************************************************************************************/

	@Override
	public void set(String name, Object value) {
		this.poolProperties.set(name, value);
		
	}

	@Override
	public Object get(String name) {
		return this.poolProperties.get(name);
	}

	@Override
	public void setHost(String host) {
		this.poolProperties.setHost(host);
		
	}

	@Override
	public String getHost() {
		return this.poolProperties.getHost();
	}

	@Override
	public void setPort(int port) {
		this.poolProperties.setPort(port);		
	}

	@Override
	public int getPort() {
		return this.poolProperties.getPort();
	}

	@Override
	public void setUrl(String url) {
		this.poolProperties.setUrl(url);
	}

	@Override
	public String getUrl() {
		return this.poolProperties.getUrl();
	}

	@Override
	public void setFramed(boolean framed) {
		this.poolProperties.setFramed(framed);
	}

	@Override
	public boolean isFramed() {
		return this.poolProperties.isFramed();
	}

	@Override
	public void setAutomaticHostDiscovery(boolean autoDiscovery) {
		this.poolProperties.setAutomaticHostDiscovery(autoDiscovery);
	}

	@Override
	public boolean isAutomaticHostDiscovery() {
		return this.poolProperties.isAutomaticHostDiscovery();
	}

	@Override
	public void setFailoverPolicy(HostFailoverPolicy failoverPolicy) {
		this.poolProperties.setFailoverPolicy(failoverPolicy);
		
	}

	@Override
	public HostFailoverPolicy getFailoverPolicy() {
		return this.poolProperties.getFailoverPolicy();
	}

	@Override
	public void setAbandonWhenPercentageFull(int percentage) {
		this.poolProperties.setAbandonWhenPercentageFull(percentage);
	}

	@Override
	public int getAbandonWhenPercentageFull() {
		return this.poolProperties.getAbandonWhenPercentageFull();
	}

	@Override
	public boolean isFairQueue() {
		return this.poolProperties.isFairQueue();
	}

	@Override
	public void setFairQueue(boolean fairQueue) {
		this.poolProperties.setFairQueue(fairQueue);
	}

	@Override
	public int getInitialSize() {
		return this.poolProperties.getInitialSize();
	}

	@Override
	public void setInitialSize(int initialSize) {
		this.poolProperties.setInitialSize(initialSize);
	}

	@Override
	public boolean isLogAbandoned() {
		return this.poolProperties.isLogAbandoned();
	}

	@Override
	public void setLogAbandoned(boolean logAbandoned) {
		this.poolProperties.setLogAbandoned(logAbandoned);
	}

	@Override
	public int getMaxActive() {
		return this.poolProperties.getMaxActive();
	}

	@Override
	public void setMaxActive(int maxActive) {
		this.poolProperties.setMaxActive(maxActive);
	}

	@Override
	public int getMaxIdle() {
		return this.poolProperties.getMaxIdle();
	}

	@Override
	public void setMaxIdle(int maxIdle) {
		this.poolProperties.setMaxIdle(maxIdle);
	}

	@Override
	public int getMaxWait() {
		return this.poolProperties.getMaxWait();
	}

	@Override
	public void setMaxWait(int maxWait) {
		this.poolProperties.setMaxWait(maxWait);
		
	}

	@Override
	public int getMinEvictableIdleTimeMillis() {
		return this.poolProperties.getMinEvictableIdleTimeMillis();
	}

	@Override
	public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
		this.poolProperties.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
	}

	@Override
	public int getMinIdle() {
		return this.poolProperties.getMinIdle();
	}

	@Override
	public void setMinIdle(int minIdle) {
		this.poolProperties.setMinIdle(minIdle);
	}

	@Override
	public String getName() {
		return this.poolProperties.getName();
	}

	@Override
	public void setName(String name) {
		this.poolProperties.setName(name);
		
	}

	@Override
	public int getNumTestsPerEvictionRun() {
		return this.poolProperties.getNumTestsPerEvictionRun();
	}

	@Override
	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		this.poolProperties.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
	}

	@Override
	public String getPassword() {
		return this.poolProperties.getPassword();
	}

	@Override
	public void setPassword(String password) {
		this.poolProperties.setPassword(password);
	}

	@Override
	public String getPoolName() {
		return this.poolProperties.getPoolName();
	}
	
	@Override
	public String getKeySpace() {
		return this.poolProperties.getKeySpace();
	}

	@Override
	public void setKeySpace(String keySpace) {
		this.poolProperties.setKeySpace(keySpace);
	}

	@Override
	public String getUsername() {
		return this.poolProperties.getUsername();
	}

	@Override
	public void setUsername(String username) {
		this.poolProperties.setUsername(username);
		
	}

	@Override
	public boolean isRemoveAbandoned() {
		return this.poolProperties.isRemoveAbandoned();
	}

	@Override
	public void setRemoveAbandoned(boolean removeAbandoned) {
		this.poolProperties.setRemoveAbandoned(removeAbandoned);
	}

	@Override
	public void setRemoveAbandonedTimeout(int removeAbandonedTimeout) {
		this.poolProperties.setRemoveAbandonedTimeout(removeAbandonedTimeout);
	}

	@Override
	public int getRemoveAbandonedTimeout() {
		return this.poolProperties.getRemoveAbandonedTimeout();
	}

	@Override
	public boolean isTestOnBorrow() {
		return this.poolProperties.isTestOnBorrow();
	}

	@Override
	public void setTestOnBorrow(boolean testOnBorrow) {
		this.poolProperties.setTestOnBorrow(testOnBorrow);
	}

	@Override
	public boolean isTestOnReturn() {
		return this.poolProperties.isTestOnBorrow();
	}

	@Override
	public void setTestOnReturn(boolean testOnReturn) {
		this.poolProperties.setTestOnReturn(testOnReturn);
	}

	@Override
	public boolean isTestWhileIdle() {
		return this.poolProperties.isTestWhileIdle();
	}

	@Override
	public void setTestWhileIdle(boolean testWhileIdle) {
		this.poolProperties.setTestWhileIdle(testWhileIdle);
	}

	@Override
	public int getTimeBetweenEvictionRunsMillis() {
		return this.poolProperties.getTimeBetweenEvictionRunsMillis();
	}

	@Override
	public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
		this.poolProperties.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
	}

	@Override
	public long getValidationInterval() {
		return this.poolProperties.getValidationInterval();
	}

	@Override
	public void setValidationInterval(long validationInterval) {
		this.poolProperties.setValidationInterval(validationInterval);
	}

	@Override
	public boolean isTestOnConnect() {
		return this.poolProperties.isTestOnConnect();
	}

	@Override
	public void setTestOnConnect(boolean testOnConnect) {
		this.poolProperties.setTestOnConnect(testOnConnect);
	}

	@Override
	public boolean isJmxEnabled() {
		return this.poolProperties.isJmxEnabled();
	}

	@Override
	public void setJmxEnabled(boolean jmxEnabled) {
		this.poolProperties.setJmxEnabled(jmxEnabled);
	}

	@Override
	public boolean isPoolSweeperEnabled() {
		return this.poolProperties.isPoolSweeperEnabled();
	}

	@Override
	public boolean isUseEquals() {
		return this.poolProperties.isUseEquals();
	}

	@Override
	public void setUseEquals(boolean useEquals) {
		this.poolProperties.setUseEquals(useEquals);
	}

	@Override
	public long getMaxAge() {
		return this.poolProperties.getMaxAge();
	}

	@Override
	public void setMaxAge(long maxAge) {
		this.poolProperties.setMaxAge(maxAge);
	}

	@Override
	public boolean getUseLock() {
		return this.poolProperties.getUseLock();
	}

	@Override
	public void setUseLock(boolean useLock) {
		this.poolProperties.setUseLock(useLock);
	}

	@Override
	public void setSuspectTimeout(int seconds) {
		this.poolProperties.setSuspectTimeout(seconds);
	}

	@Override
	public int getSuspectTimeout() {
		return this.poolProperties.getSuspectTimeout();
	}

	@Override
	public int getSocketTimeout() {
		return this.poolProperties.getSocketTimeout();
	}

	@Override
	public void setSocketTimeout(int socketTimeout) {
		this.poolProperties.setSocketTimeout(socketTimeout);
	}

	@Override
	public String[] getConfiguredHosts() {
		return this.poolProperties.getConfiguredHosts();
	}

	@Override
	public long getHostRetryInterval() {
		return this.poolProperties.getHostRetryInterval();
	}

	@Override
	public void setHostRetryInterval(long hostRetryInterval) {
		this.poolProperties.setHostRetryInterval(hostRetryInterval);
	}

	@Override
	public void setDataSourceJNDI(String jndiDS) {
		this.poolProperties.setDataSourceJNDI(jndiDS);
	}

	@Override
	public String getDataSourceJNDI() {
		return this.poolProperties.getDataSourceJNDI();
	}

	@Override
	public void setDataSource(Object ds) {
		this.poolProperties.setDataSource(ds);
	}

	@Override
	public Object getDataSource() {
		return this.poolProperties.getDataSource();
	}
}
