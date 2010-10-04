package net.dataforte.cassandra.pool;

public class CassandraHost {
	public enum Status {
		ONLINE, OFFLINE
	};

	String address;
	long lastSuccess;
	long lastFailure;

	public CassandraHost(String address) {
		setAddress(address);
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public long getLastSuccess() {
		return lastSuccess;
	}

	public void setLastSuccess(long lastSuccess) {
		this.lastSuccess = lastSuccess;
	}

	public long getLastFailure() {
		return lastFailure;
	}

	public void setLastFailure(long lastFailure) {
		this.lastFailure = lastFailure;
	}

}
