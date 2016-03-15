package org.terifan.raccoon;

import org.terifan.raccoon.ByteBufferMap;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import static tests.__TestUtils.*;


public class ByteBufferMapNGTest
{
	@Test
	public void testSinglePutGet()
	{
		ByteBufferMap map = new ByteBufferMap(4096);

		byte[] key = tb();
		byte[] value = tb();

		map.put(new ByteBufferMap.Entry(key, value, 0));
		
		ByteBufferMap.Entry entry = new ByteBufferMap.Entry(key);

		assertTrue(map.get(entry));
		assertEquals(entry.getValue(), value);
	}


	@Test
	public void testFillBuffer()
	{
		ByteBufferMap map = new ByteBufferMap(4096);

		HashMap<byte[],byte[]> values = new HashMap<>();

		for (int i = 0; i < 1000; i++)
		{
			byte[] key = tb();
			byte[] value = tb();

			if (!map.put(new ByteBufferMap.Entry(key, value, 0)))
			{
				break;
			}

			values.put(key,value);
		}

		for (Map.Entry<byte[],byte[]> entry : values.entrySet())
		{
			ByteBufferMap.Entry entry1 = new ByteBufferMap.Entry(entry.getKey());
			assertTrue(map.get(entry1));
			assertEquals(entry1.getValue(), entry.getValue());
		}
	}
}
