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

package net.dataforte.cassandra.pool.jmx;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationListener;
import javax.management.ReflectionException;

import net.dataforte.cassandra.pool.PoolConfiguration;
import net.dataforte.cassandra.pool.PoolProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionPoolMBean extends NotificationBroadcasterSupport implements DynamicMBean {
	/**
	 * logger
	 */
	private static final Logger log = LoggerFactory.getLogger(ConnectionPoolMBean.class);

	/**
	 * the connection pool
	 */
	protected net.dataforte.cassandra.pool.ConnectionPool pool = null;
	/**
	 * sequence for JMX notifications
	 */
	protected AtomicInteger sequence = new AtomicInteger(0);

	/**
	 * Listeners that are local and interested in our notifications, no need for
	 * JMX
	 */
	protected ConcurrentLinkedQueue<NotificationListener> listeners = new ConcurrentLinkedQueue<NotificationListener>();

	public ConnectionPoolMBean(net.dataforte.cassandra.pool.ConnectionPool pool) {
		super();
		this.pool = pool;
	}

	public net.dataforte.cassandra.pool.ConnectionPool getPool() {
		return pool;
	}

	public PoolConfiguration getPoolProperties() {
		return pool.getPoolProperties();
	}

	@Override
	public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
		if(log.isDebugEnabled()) {
			log.debug("Invoking {}", actionName);
		}
		//TODO
		return null;
	}

	@Override
	public MBeanInfo getMBeanInfo() {
		List<MBeanAttributeInfo> attributes = new ArrayList<MBeanAttributeInfo>();
		
		// Dynamically add all of the properties specified in the PoolProperties
		try {
			BeanInfo beanInfo = Introspector.getBeanInfo(PoolProperties.class);
			for (PropertyDescriptor pd : beanInfo.getPropertyDescriptors()) {
				// skip a few "sensitive" attributes
				if(!("password".equals(pd.getName())||"class".equals(pd.getName()))) {
					attributes.add(new MBeanAttributeInfo(pd.getName(), pd.getPropertyType().getName(), pd.getDisplayName(), true, true, false));
				}
			}
		} catch(Exception e) {
			// Should not happen
		}
		
		// Add the ConnectionPool properties
		attributes.add(new MBeanAttributeInfo("size", "int", "size", true, false, false));
		attributes.add(new MBeanAttributeInfo("active", "int", "active", true, false, false));
		attributes.add(new MBeanAttributeInfo("idle", "int", "idle", true, false, false));
		attributes.add(new MBeanAttributeInfo("waitCount", "int", "waitCount", true, false, false));		
		
		List<MBeanOperationInfo> operations = new ArrayList<MBeanOperationInfo>();
		String ops[] = new String[]{ "checkIdle", "checkAbandoned", "testIdle" };
		
		for(String op : ops) {
			try {
				operations.add(new MBeanOperationInfo(op, this.getClass().getMethod(op, null)));
			} catch (Exception e) {
				// Will not happen
			}
		}
		
		
		return new MBeanInfo(this.getClass().getName(), "Cassandra Connection Pool", attributes.toArray(new MBeanAttributeInfo[0]), null, operations.toArray(new MBeanOperationInfo[0]), null);
	}
	

	@Override
	public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException {
		pool.getPoolProperties().set(attribute.getName(), attribute.getValue());
	}

	@Override
	public AttributeList setAttributes(AttributeList attributes) {
		AttributeList list = new AttributeList();
		for(Attribute attribute : attributes.asList()) {
			try {
				setAttribute(attribute);
				list.add(attribute);
			} catch (Exception e) {
				// setAttribute will have already logged
			}
		}
		return list;
	}

	@Override
	public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException {
		if("size".equals(attribute)) {
			return pool.getSize();
		} else if("active".equals(attribute)) {
			return pool.getActive();
		} else if("idle".equals(attribute)) {
			return pool.getIdle();
		} else if("waitCount".equals(attribute)) {
			return pool.getWaitCount();
		} else {
			return pool.getPoolProperties().get(attribute);
		}
	}

	@Override
	public AttributeList getAttributes(String[] attributes) {
		AttributeList list = new AttributeList();
		for (String attribute : attributes) {
			try {
				list.add(new Attribute(attribute, getAttribute(attribute)));
			} catch (Exception e) {
				log.error("Error while adding attribute " + attribute, e);
			}
		}
		return list;
	}

	// =================================================================
	// NOTIFICATION INFO
	// =================================================================
	public static final String NOTIFY_INIT = "INIT FAILED";
	public static final String NOTIFY_CONNECT = "CONNECTION FAILED";
	public static final String NOTIFY_ABANDON = "CONNECTION ABANDONED";
	public static final String SUSPECT_ABANDONED_NOTIFICATION = "SUSPECT CONNECTION ABANDONED";

	@Override
	public MBeanNotificationInfo[] getNotificationInfo() {
		MBeanNotificationInfo[] pres = super.getNotificationInfo();
		MBeanNotificationInfo[] loc = getDefaultNotificationInfo();
		MBeanNotificationInfo[] aug = new MBeanNotificationInfo[pres.length + loc.length];
		if (pres.length > 0)
			System.arraycopy(pres, 0, aug, 0, pres.length);
		if (loc.length > 0)
			System.arraycopy(loc, 0, aug, pres.length, loc.length);
		return aug;
	}

	public static MBeanNotificationInfo[] getDefaultNotificationInfo() {
		String[] types = new String[] { NOTIFY_INIT, NOTIFY_CONNECT, NOTIFY_ABANDON, SUSPECT_ABANDONED_NOTIFICATION };
		String name = Notification.class.getName();
		String description = "A connection pool error condition was met.";
		MBeanNotificationInfo info = new MBeanNotificationInfo(types, name, description);
		return new MBeanNotificationInfo[] { info };
	}

	/**
	 * Return true if the notification was sent successfully, false otherwise.
	 * 
	 * @param type
	 * @param message
	 * @return true if the notification succeeded
	 */	
	public boolean notify(final String type, String message) {
		try {
			Notification n = new Notification(type, this, sequence.incrementAndGet(), System.currentTimeMillis(), "[" + type + "] " + message);
			sendNotification(n);
			for (NotificationListener listener : listeners) {
				listener.handleNotification(n, this);
			}
			return true;
		} catch (Exception x) {
			if (log.isDebugEnabled()) {
				log.debug("Notify failed. Type=" + type + "; Message=" + message, x);
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

	// =================================================================
	// POOL OPERATIONS
	// =================================================================
	public void checkIdle() {
		pool.checkIdle();
	}

	public void checkAbandoned() {
		pool.checkAbandoned();
	}

	public void testIdle() {
		pool.testAllIdle();
	}

}
