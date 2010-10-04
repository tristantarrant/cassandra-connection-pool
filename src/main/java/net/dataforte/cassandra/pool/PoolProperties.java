/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
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
package net.dataforte.cassandra.pool;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;



/**
 * @author Tristan Tarrant
 */
public class PoolProperties implements PoolConfiguration {
    protected static AtomicInteger poolCounter = new AtomicInteger(0);
    
    protected String host;
    protected String[] configuredHosts;
    protected int port = 9160;
    protected boolean framed = false;
    protected boolean automaticHostDiscovery = true;
    protected int socketTimeout = 5000;

	protected int initialSize = 10;
    protected int maxActive = 100;
    protected int maxIdle = maxActive;
    protected int minIdle = initialSize;
    protected int maxWait = 30000;
    
    protected boolean testOnBorrow = false;
    protected boolean testOnReturn = false;
    protected boolean testWhileIdle = false;
    protected int timeBetweenEvictionRunsMillis = 5000;
    protected int numTestsPerEvictionRun;
    protected int minEvictableIdleTimeMillis = 60000;
    protected final boolean accessToUnderlyingConnectionAllowed = true;
    protected boolean removeAbandoned = false;
    protected int removeAbandonedTimeout = 60;
    protected boolean logAbandoned = false;
    protected String name = "Cassandra Connection Pool["+(poolCounter.incrementAndGet())+"-"+System.identityHashCode(PoolProperties.class)+"]";
    protected String password;
    protected String username;
    protected long validationInterval = 30000;
    protected boolean jmxEnabled = true;
    
    protected boolean testOnConnect =false;
    protected boolean fairQueue = true;
    protected boolean useEquals = true;
    protected int abandonWhenPercentageFull = 0;
    protected long maxAge = 0;
    protected boolean useLock = false;
    protected int suspectTimeout = 0;
    
    /** 
     * {@inheritDoc}
     */
    @Override
    public void setAbandonWhenPercentageFull(int percentage) {
        if (percentage<0) abandonWhenPercentageFull = 0;
        else if (percentage>100) abandonWhenPercentageFull = 100;
        else abandonWhenPercentageFull = percentage;
    }
    
    /** 
     * {@inheritDoc}
     */
    @Override
    public int getAbandonWhenPercentageFull() {
        return abandonWhenPercentageFull;
    }
    
    /** 
     * {@inheritDoc}
     */
    @Override
    public boolean isFairQueue() {
        return fairQueue;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setFairQueue(boolean fairQueue) {
        this.fairQueue = fairQueue;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public boolean isAccessToUnderlyingConnectionAllowed() {
        return accessToUnderlyingConnectionAllowed;
    }
    
    @Override
    public boolean isAutomaticHostDiscovery() {
		return automaticHostDiscovery;
	}

	public void setAutomaticHostDiscovery(boolean automaticHostDiscovery) {
		this.automaticHostDiscovery = automaticHostDiscovery;
	}

    /** 
     * {@inheritDoc}
     */
    @Override
    public int getInitialSize() {
        return initialSize;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public boolean isLogAbandoned() {
        return logAbandoned;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public int getMaxActive() {
        return maxActive;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public int getMaxIdle() {
        return maxIdle;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public int getMaxWait() {
        return maxWait;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public int getMinEvictableIdleTimeMillis() {
        return minEvictableIdleTimeMillis;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public int getMinIdle() {
        return minIdle;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return name;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public int getNumTestsPerEvictionRun() {
        return numTestsPerEvictionRun;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public String getPassword() {
        return password;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public String getPoolName() {
        return getName();
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public boolean isRemoveAbandoned() {
        return removeAbandoned;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public int getRemoveAbandonedTimeout() {
        return removeAbandonedTimeout;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public boolean isTestOnReturn() {
        return testOnReturn;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public boolean isTestWhileIdle() {
        return testWhileIdle;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public int getTimeBetweenEvictionRunsMillis() {
        return timeBetweenEvictionRunsMillis;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public String getUsername() {
        return username;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public long getValidationInterval() {
        return validationInterval;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public boolean isTestOnConnect() {
        return testOnConnect;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setAccessToUnderlyingConnectionAllowed(boolean accessToUnderlyingConnectionAllowed) {
        // NOOP
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setInitialSize(int initialSize) {
        this.initialSize = initialSize;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setLogAbandoned(boolean logAbandoned) {
        this.logAbandoned = logAbandoned;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setMaxWait(int maxWait) {
        this.maxWait = maxWait;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
        this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setName(String name) {
        this.name = name;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
        this.numTestsPerEvictionRun = numTestsPerEvictionRun;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setPassword(String password) {
        this.password = password;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setRemoveAbandoned(boolean removeAbandoned) {
        this.removeAbandoned = removeAbandoned;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setRemoveAbandonedTimeout(int removeAbandonedTimeout) {
        this.removeAbandonedTimeout = removeAbandonedTimeout;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setTestWhileIdle(boolean testWhileIdle) {
        this.testWhileIdle = testWhileIdle;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setTestOnReturn(boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setTimeBetweenEvictionRunsMillis(int
                                                 timeBetweenEvictionRunsMillis) {
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setUsername(String username) {
        this.username = username;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setValidationInterval(long validationInterval) {
        this.validationInterval = validationInterval;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setTestOnConnect(boolean testOnConnect) {
        this.testOnConnect = testOnConnect;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("ConnectionPool[]");
        return buf.toString();
    }

    public static int getPoolCounter() {
        return poolCounter.get();
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public boolean isJmxEnabled() {
        return jmxEnabled;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setJmxEnabled(boolean jmxEnabled) {
        this.jmxEnabled = jmxEnabled;
    }    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int getSuspectTimeout() {
        return this.suspectTimeout;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSuspectTimeout(int seconds) {
        this.suspectTimeout = seconds;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public boolean isPoolSweeperEnabled() {
        boolean timer = getTimeBetweenEvictionRunsMillis()>0;
        boolean result = timer && (isRemoveAbandoned() && getRemoveAbandonedTimeout()>0);
        result = result || (timer && getSuspectTimeout()>0); 
        result = result || (timer && isTestWhileIdle());
        result = result || (timer && getMinEvictableIdleTimeMillis()>0); 
        return result;
    }
    
    public static class InterceptorProperty {
        String name;
        String value;
        public InterceptorProperty(String name, String value) {
            assert(name!=null);
            this.name = name;
            this.value = value;
        }
        public String getName() {
            return name;
        }
        public String getValue() {
            return value;
        }
        
        public boolean getValueAsBoolean(boolean def) {
            if (value==null) return def;
            if ("true".equals(value)) return true;
            if ("false".equals(value)) return false;
            return def;
        }
        
        public int getValueAsInt(int def) {
            if (value==null) return def;
            try {
                int v = Integer.parseInt(value);
                return v;
            }catch (NumberFormatException nfe) {
                return def;
            }
        }
        
        public long getValueAsLong(long def) {
            if (value==null) return def;
            try {
                return Long.parseLong(value);
            }catch (NumberFormatException nfe) {
                return def;
            }
        }

        public byte getValueAsByte(byte def) {
            if (value==null) return def;
            try {
                return Byte.parseByte(value);
            }catch (NumberFormatException nfe) {
                return def;
            }
        }
        
        public short getValueAsShort(short def) {
            if (value==null) return def;
            try {
                return Short.parseShort(value);
            }catch (NumberFormatException nfe) {
                return def;
            }
        }

        public float getValueAsFloat(float def) {
            if (value==null) return def;
            try {
                return Float.parseFloat(value);
            }catch (NumberFormatException nfe) {
                return def;
            }
        }

        public double getValueAsDouble(double def) {
            if (value==null) return def;
            try {
                return Double.parseDouble(value);
            }catch (NumberFormatException nfe) {
                return def;
            }
        }
 
        public char getValueAschar(char def) {
            if (value==null) return def;
            try {
                return value.charAt(0);
            }catch (StringIndexOutOfBoundsException nfe) {
                return def;
            }
        }
        @Override
        public int hashCode() {
            return name.hashCode();
        }
        @Override
        public boolean equals(Object o) {
            if (o==this) return true;
            if (o instanceof InterceptorProperty) {
                InterceptorProperty other = (InterceptorProperty)o;
                return other.name.equals(this.name);
            }
            return false;
        }
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public boolean isUseEquals() {
        return useEquals;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setUseEquals(boolean useEquals) {
        this.useEquals = useEquals;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public long getMaxAge() {
        return maxAge;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setMaxAge(long maxAge) {
        this.maxAge = maxAge;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public boolean getUseLock() {
        return useLock;
    }

    /** 
     * {@inheritDoc}
     */
    @Override
    public void setUseLock(boolean useLock) {
        this.useLock = useLock;
    }    
        
    public static Properties getProperties(String propText, Properties props) {
        if (props==null) props = new Properties();
        if (propText != null) {
            try {
                props.load(new ByteArrayInputStream(propText.replace(';', '\n').getBytes()));
            }catch (IOException x) {
                throw new RuntimeException(x);
            }
        }
        return props;
    }

	@Override
	public void setHost(String host) {
		this.host = host;
		String[] hs = this.host.split(";");
		configuredHosts = new String[hs.length];
		
		for(int i=0; i<hs.length; i++) {
			configuredHosts[i] = hs[i].trim();
		}
	}

	@Override
	public String getHost() {
		StringBuilder sb = new StringBuilder();
		for(int i=0; i<configuredHosts.length; i++) {
			if(i>0)
				sb.append(",");
			sb.append(configuredHosts);		
		}
		return sb.toString();
	}

	@Override
	public void setPort(int port) {
		this.port = port;
		
	}

	@Override
	public int getPort() {
		return port;
	}

	@Override
	public void setFramed(boolean framed) {
		this.framed = framed;
	}

	@Override
	public boolean isFramed() {
		return framed;
	}

	@Override
	public int getSocketTimeout() {
		return socketTimeout;
	}

	@Override
	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	@Override
	public String[] getConfiguredHosts() {
		return configuredHosts;
	}
}
