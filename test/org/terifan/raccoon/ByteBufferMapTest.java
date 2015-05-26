package org.terifan.raccoon;

import org.terifan.raccoon.ByteBufferMap.PutResult;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;
import static org.testng.Assert.*;


public class ByteBufferMapTest
{
	public ByteBufferMapTest()
	{
	}


	@Test
	public void testSomeMethod()
	{
		ByteBufferMap map = new ByteBufferMap(4096);
		PutResult result = new PutResult();
		map.put(0, "key".getBytes(), "value".getBytes(), result);
		assertTrue(result.inserted);
	}
}
