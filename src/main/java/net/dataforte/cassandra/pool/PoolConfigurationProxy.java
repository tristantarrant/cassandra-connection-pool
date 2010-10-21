package net.dataforte.cassandra.pool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class PoolConfigurationProxy implements InvocationHandler {
	final PoolProperties props;	

	public PoolConfigurationProxy(final PoolProperties props) {
		this.props = props;
	}

	public static final PoolConfiguration makePoolConfigurationProxy(PoolProperties props) {
		try {			
			return (PoolConfiguration) Proxy.newProxyInstance(PoolConfiguration.class.getClassLoader(), new Class[] { PoolConfiguration.class }, new PoolConfigurationProxy(props));
		} catch (Throwable t) {
			throw new RuntimeException("Could not instantiate pool configuration proxy", t);
		}
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		return method.invoke(this.props, args);
	}
}
