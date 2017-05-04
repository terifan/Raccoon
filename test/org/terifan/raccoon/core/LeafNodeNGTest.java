package org.terifan.raccoon.core;

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import static resources.__TestUtils.*;


public class LeafNodeNGTest
{
	@Test
	public void testSinglePutGet()
	{
		LeafNode map = new LeafNode(4096);

		byte[] key = tb();
		byte[] value = tb();

		map.put(new RecordEntry(key, value, (byte)77));

		RecordEntry entry = new RecordEntry(key);

		assertTrue(map.get(entry));
		assertEquals(entry.getValue(), value);
		assertEquals(entry.getFlags(), (byte)77);
	}


	@Test
	public void testFillBuffer()
	{
		LeafNode map = new LeafNode(4096);

		HashMap<byte[],byte[]> values = new HashMap<>();

		for (int i = 0; i < 1000; i++)
		{
			byte[] key = tb();
			byte[] value = tb();

			if (!map.put(new RecordEntry(key, value, (byte)0)))
			{
				break;
			}

			values.put(key,value);
		}

		for (Map.Entry<byte[],byte[]> entry : values.entrySet())
		{
			RecordEntry entry1 = new RecordEntry(entry.getKey());
			assertTrue(map.get(entry1));
			assertEquals(entry1.getValue(), entry.getValue());
		}
	}
}
