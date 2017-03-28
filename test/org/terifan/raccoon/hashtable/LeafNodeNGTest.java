package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.hashtable.LeafEntry;
import org.terifan.raccoon.hashtable.LeafNode;
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

		map.put(new LeafEntry(key, value, (byte)77));

		LeafEntry entry = new LeafEntry(key);

		assertTrue(map.get(entry));
		assertEquals(entry.mValue, value);
		assertEquals(entry.mFlags, (byte)77);
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

			if (!map.put(new LeafEntry(key, value, (byte)0)))
			{
				break;
			}

			values.put(key,value);
		}

		for (Map.Entry<byte[],byte[]> entry : values.entrySet())
		{
			LeafEntry entry1 = new LeafEntry(entry.getKey());
			assertTrue(map.get(entry1));
			assertEquals(entry1.mValue, entry.getValue());
		}
	}
}
