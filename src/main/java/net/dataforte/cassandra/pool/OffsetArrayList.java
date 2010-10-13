package net.dataforte.cassandra.pool;

import java.util.ArrayList;
import java.util.Collection;

public class OffsetArrayList<E> extends ArrayList<E> {
	int offset;
	
	public OffsetArrayList(Collection<? extends E> c, int offset) {
		super(c);
		this.offset = offset;
	}
	
	@Override
	public E get(int index) {
		return super.get((index+offset) % super.size());
	}
	

}
