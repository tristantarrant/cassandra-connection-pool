package net.dataforte.cassandra.pool;

public class CassandraHost {
	String host;
	long lastUsed;
	boolean good;

	public CassandraHost(String host) {
		this.host = host;
		good = true;
	}

	public String getHost() {
		return host;
	}

	public void timestamp() {
		lastUsed = System.currentTimeMillis();
	}

	public long getLastUsed() {
		return lastUsed;
	}

	public boolean isGood() {
		return good;
	}

	public void setGood(boolean good) {
		this.good = good;
	}

	public String toString() {
		return "[" + host + ",status=" + good + ",timestamp=" + lastUsed + "]";
	}

}
