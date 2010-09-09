/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.dataforte.cassandra.pool.jmx;
/**
 * Derived from org.apache.tomcat.jdbc.pool.jmx.ConnectionPool by fhanik
 * 
 * @author Filip Hanik
 */
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationListener;

import net.dataforte.cassandra.pool.PoolConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConnectionPool extends NotificationBroadcasterSupport implements ConnectionPoolMBean  {
    /**
     * logger
     */
    private static final Logger log = LoggerFactory.getLogger(ConnectionPool.class);

    /**
     * the connection pool
     */
    protected net.dataforte.cassandra.pool.ConnectionPool pool = null;
    /**
     * sequence for JMX notifications
     */
    protected AtomicInteger sequence = new AtomicInteger(0);
    
    /**
     * Listeners that are local and interested in our notifications, no need for JMX
     */
    protected ConcurrentLinkedQueue<NotificationListener> listeners = new ConcurrentLinkedQueue<NotificationListener>(); 

    public ConnectionPool(net.dataforte.cassandra.pool.ConnectionPool pool) {
        super();
        this.pool = pool;
    }

    public net.dataforte.cassandra.pool.ConnectionPool getPool() {
        return pool;
    }
    
    public PoolConfiguration getPoolProperties() {
        return pool.getPoolProperties();
    }
    
    //=================================================================
    //       NOTIFICATION INFO
    //=================================================================
    public static final String NOTIFY_INIT = "INIT FAILED";
    public static final String NOTIFY_CONNECT = "CONNECTION FAILED";
    public static final String NOTIFY_ABANDON = "CONNECTION ABANDONED";
    public static final String SUSPECT_ABANDONED_NOTIFICATION = "SUSPECT CONNETION ABANDONED";


    public MBeanNotificationInfo[] getNotificationInfo() { 
        MBeanNotificationInfo[] pres = super.getNotificationInfo();
        MBeanNotificationInfo[] loc = getDefaultNotificationInfo();
        MBeanNotificationInfo[] aug = new MBeanNotificationInfo[pres.length + loc.length];
        if (pres.length>0) System.arraycopy(pres, 0, aug, 0, pres.length);
        if (loc.length >0) System.arraycopy(loc, 0, aug, pres.length, loc.length);    
        return aug; 
    } 
    
    public static MBeanNotificationInfo[] getDefaultNotificationInfo() {
        String[] types = new String[] {NOTIFY_INIT, NOTIFY_CONNECT, NOTIFY_ABANDON, SUSPECT_ABANDONED_NOTIFICATION}; 
        String name = Notification.class.getName(); 
        String description = "A connection pool error condition was met."; 
        MBeanNotificationInfo info = new MBeanNotificationInfo(types, name, description); 
        return new MBeanNotificationInfo[] {info};
    }
    
    /**
     * Return true if the notification was sent successfully, false otherwise.
     * @param type
     * @param message
     * @return true if the notification succeeded
     */
    public boolean notify(final String type, String message) {
        try {
            Notification n = new Notification(
                    type,
                    this,
                    sequence.incrementAndGet(),
                    System.currentTimeMillis(),
                    "["+type+"] "+message);
            sendNotification(n);
            for (NotificationListener listener : listeners) {
                listener.handleNotification(n,this);
            }
            return true;
        }catch (Exception x) {
            if (log.isDebugEnabled()) {
                log.debug("Notify failed. Type="+type+"; Message="+message,x);
            }
            return false;
        }
        
    }
    
    public void addListener(NotificationListener list) {
        listeners.add(list);
    }
    
    public boolean removeListener(NotificationListener list) {
        return listeners.remove(list);
    }
    
    //=================================================================
    //       POOL STATS
    //=================================================================

    public int getSize() {
        return pool.getSize();
    }

    public int getIdle() {
        return pool.getIdle();
    }

    public int getActive() {
        return pool.getActive();
    }
    
    public int getNumIdle() {
        return getIdle();
    }
    
    public int getNumActive() {
        return getActive();
    }
    
    public int getWaitCount() {
        return pool.getWaitCount();
    }

    //=================================================================
    //       POOL OPERATIONS
    //=================================================================
    public void checkIdle() {
        pool.checkIdle();
    }

    public void checkAbandoned() {
        pool.checkAbandoned();
    }

    public void testIdle() {
        pool.testAllIdle();
    }
    //=================================================================
    //       POOL PROPERTIES
    //=================================================================
    //=========================================================
    //  PROPERTIES / CONFIGURATION
    //=========================================================    

    public int getInitialSize() {
        return getPoolProperties().getInitialSize();
    }

    public int getMaxActive() {
        return getPoolProperties().getMaxActive();
    }

    public int getMaxIdle() {
        return getPoolProperties().getMaxIdle();
    }

    public int getMaxWait() {
        return getPoolProperties().getMaxWait();
    }

    public int getMinEvictableIdleTimeMillis() {
        return getPoolProperties().getMinEvictableIdleTimeMillis();
    }

    public int getMinIdle() {
        return getPoolProperties().getMinIdle();
    }
    
    public long getMaxAge() {
        return getPoolProperties().getMaxAge();
    }    

    public String getName() {
        return this.getPoolName();
    }

    public int getNumTestsPerEvictionRun() {
        return getPoolProperties().getNumTestsPerEvictionRun();
    }

    /**
     * @return DOES NOT RETURN THE PASSWORD, IT WOULD SHOW UP IN JMX
     */
    public String getPassword() {
        return "Password not available as DataSource/JMX operation.";
    }

    public int getRemoveAbandonedTimeout() {
        return getPoolProperties().getRemoveAbandonedTimeout();
    }


    public int getTimeBetweenEvictionRunsMillis() {
        return getPoolProperties().getTimeBetweenEvictionRunsMillis();
    }

    public String getUsername() {
        return getPoolProperties().getUsername();
    }

    public long getValidationInterval() {
        return getPoolProperties().getValidationInterval();
    }

    public boolean isAccessToUnderlyingConnectionAllowed() {
        return getPoolProperties().isAccessToUnderlyingConnectionAllowed();
    }

    public boolean isLogAbandoned() {
        return getPoolProperties().isLogAbandoned();
    }

    public boolean isPoolSweeperEnabled() {
        return getPoolProperties().isPoolSweeperEnabled();
    }

    public boolean isRemoveAbandoned() {
        return getPoolProperties().isRemoveAbandoned();
    }

    public int getAbandonWhenPercentageFull() {
        return getPoolProperties().getAbandonWhenPercentageFull();
    }

    public boolean isTestOnBorrow() {
        return getPoolProperties().isTestOnBorrow();
    }

    public boolean isTestOnConnect() {
        return getPoolProperties().isTestOnConnect();
    }

    public boolean isTestOnReturn() {
        return getPoolProperties().isTestOnReturn();
    }

    public boolean isTestWhileIdle() {
        return getPoolProperties().isTestWhileIdle();
    }

    public boolean getUseLock() {
        return getPoolProperties().getUseLock();
    }

    public boolean isFairQueue() {
        return getPoolProperties().isFairQueue();
    }

    public boolean isJmxEnabled() {
        return getPoolProperties().isJmxEnabled();
    }

    public boolean isUseEquals() {
        return getPoolProperties().isUseEquals();
    }

    public void setAbandonWhenPercentageFull(int percentage) {
        getPoolProperties().setAbandonWhenPercentageFull(percentage);
    }

    public void setAccessToUnderlyingConnectionAllowed(boolean accessToUnderlyingConnectionAllowed) {
        getPoolProperties().setAccessToUnderlyingConnectionAllowed(accessToUnderlyingConnectionAllowed);
    }

    public void setMaxAge(long maxAge) {
        getPoolProperties().setMaxAge(maxAge);
    }

    public void setName(String name) {
        getPoolProperties().setName(name);
    }

    public String getPoolName() {
        return getPoolProperties().getName();
    }
        
    @Override
    public void setFairQueue(boolean fairQueue) {
        getPoolProperties().setFairQueue(fairQueue);
    }

    @Override
    public void setInitialSize(int initialSize) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setJmxEnabled(boolean jmxEnabled) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setLogAbandoned(boolean logAbandoned) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setMaxActive(int maxActive) {
        // TODO Auto-generated method stub
        
    }

    @Override 
    public void setMaxIdle(int maxIdle) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setMaxWait(int maxWait) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setMinIdle(int minIdle) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setPassword(String password) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setRemoveAbandoned(boolean removeAbandoned) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setRemoveAbandonedTimeout(int removeAbandonedTimeout) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setTestOnBorrow(boolean testOnBorrow) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setTestOnConnect(boolean testOnConnect) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setTestOnReturn(boolean testOnReturn) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setTestWhileIdle(boolean testWhileIdle) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setHost(String host) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setUseEquals(boolean useEquals) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setUseLock(boolean useLock) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setUsername(String username) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setValidationInterval(long validationInterval) {
        // TODO Auto-generated method stub
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getSuspectTimeout() {
        return getPoolProperties().getSuspectTimeout(); 
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSuspectTimeout(int seconds) {
        //no op
    }

	@Override
	public String getHost() {
		return getPoolProperties().getHost();
	}

	@Override
	public void setPort(int port) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getPort() {
		return getPoolProperties().getPort();
	}

	@Override
	public void setFramed(boolean framed) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean getFramed() {
		return getPoolProperties().getFramed();
	}


}
