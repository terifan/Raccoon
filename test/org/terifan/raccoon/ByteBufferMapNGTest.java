package org.terifan.raccoon;

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import static org.terifan.raccoon.serialization.TestUtils.*;
import org.terifan.raccoon.util.Log;


public class ByteBufferMapNGTest
{
	public ByteBufferMapNGTest()
	{
	}


	@Test
	public void testSinglePutGet()
	{
		ByteBufferMap map = new ByteBufferMap(4096);
		
		byte[] key = tb();
		byte[] value = tb();

		map.put(key, value);

		assertEquals(map.get(key), value);
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

			byte[] old = map.put(key, value);

			if (old == ByteBufferMap.OVERFLOW)
			{
				break;
			}

			values.put(key,value);
		}

		for (Map.Entry<byte[],byte[]> entry : values.entrySet())
		{
			assertEquals(map.get(entry.getKey()), entry.getValue());
		}
	}
}
