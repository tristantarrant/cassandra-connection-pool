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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CassandraHost other = (CassandraHost) obj;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		return true;
	}

}
