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

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.apache.cassandra.thrift.CassandraThriftDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceFactory implements ObjectFactory {
	private static final Logger log = LoggerFactory.getLogger(DataSourceFactory.class);

	public static final String OBJECT_NAME = "object_name";

	@Override
	public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment) throws Exception {

		if ((obj == null) || !(obj instanceof Reference)) {
			return null;
		}
		Reference ref = (Reference) obj;
		boolean ok = CassandraThriftDataSource.class.getName().equals(ref.getClassName()) || DataSource.class.getName().equals(ref.getClassName());

		if (!ok) {
			log.warn(ref.getClassName() + " is not a valid class name/type for this JNDI factory.");
			return null;
		}

		Map<String, String> properties = new HashMap<String, String>();
		for (String propertyName : PoolProperties.getPropertyNames()) {
			RefAddr ra = ref.get(propertyName);
			if (ra != null) {
				String propertyValue = ra.getContent().toString();
				properties.put(propertyName, propertyValue);
			}
		}

		return createDataSource(properties, nameCtx);
	}

	public static PoolConfiguration parsePoolProperties(Map<String, String> properties) throws IOException {
		PoolConfiguration poolProperties = new PoolProperties();
		for (Entry<String, String> property : properties.entrySet()) {
			poolProperties.set(property.getKey(), property.getValue());
		}
		return poolProperties;
	}

	/**
	 * Creates and configures a {@link DataSource} instance based on the given
	 * properties.
	 * 
	 * @param properties
	 *            the datasource configuration properties
	 * @throws Exception
	 *             if an error occurs creating the data source
	 */
	public CassandraThriftDataSource createDataSource(Map<String, String> properties) throws Exception {
		return createDataSource(properties, null);
	}

	public CassandraThriftDataSource createDataSource(Map<String, String> properties, Context context) throws Exception {
		PoolConfiguration poolProperties = DataSourceFactory.parsePoolProperties(properties);
		if (poolProperties.getDataSourceJNDI() != null && poolProperties.getDataSource() == null) {
			performJNDILookup(context, poolProperties);
		}
		DataSource dataSource = new DataSource(poolProperties);
		// initialise the pool itself
		dataSource.createPool();
		// Return the configured DataSource instance
		return dataSource;
	}

	public void performJNDILookup(Context context, PoolConfiguration poolProperties) {
		Object jndiDS = null;
		try {
			if (context != null) {
				jndiDS = context.lookup(poolProperties.getDataSourceJNDI());
			} else {
				log.warn("dataSourceJNDI property is configued, but local JNDI context is null.");
			}
		} catch (NamingException e) {
			log.debug("The name \"" + poolProperties.getDataSourceJNDI() + "\" can not be found in the local context.");
		}
		if (jndiDS == null) {
			try {
				context = (Context) (new InitialContext());
				jndiDS = context.lookup(poolProperties.getDataSourceJNDI());
			} catch (NamingException e) {
				log.warn("The name \"" + poolProperties.getDataSourceJNDI() + "\" can not be found in the InitialContext.");
			}
		}
		if (jndiDS != null) {
			poolProperties.setDataSource(jndiDS);
		}
	}

	/**
	 * <p>
	 * Parse properties from the string. Format of the string must be
	 * [propertyName=property;]*
	 * <p>
	 * 
	 * @param propText
	 * @return Properties
	 * @throws Exception
	 */
	static protected Map<String, String> getProperties(String propText) throws IOException {
		return PoolProperties.getProperties(propText, null);
	}

}
