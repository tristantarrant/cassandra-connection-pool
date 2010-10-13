package net.dataforte.cassandra.pool;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;


public class OffsetArrayListTest {

	@Test
	public void testOffsetArrayList() {
		List<String> a = new ArrayList<String>();
		a.add("a");
		a.add("b");
		a.add("c");
		a.add("d");
		a.add("e");
		a.add("f");
		OffsetArrayList<String> list = new OffsetArrayList<String>(a, 3);
		Assert.assertEquals("d", list.get(0));
		Assert.assertEquals("a", list.get(3));		
	}
}
