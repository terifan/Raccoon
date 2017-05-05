package org.terifan.raccoon.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import static resources.__TestUtils.*;


public class ArrayMapNGTest
{
	@Test
	public void testSinglePutGet()
	{
		ArrayMap map = new ArrayMap(4096);

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
		ArrayMap map = new ArrayMap(65536);

		HashMap<String, byte[]> values = new HashMap<>();

		for (int i = 0; i < 10000; i++)
		{
			byte[] key = tb();
			byte[] value = tb();

			if (!map.put(new RecordEntry(key, value, (byte)0)))
			{
				break;
			}

			values.put(new String(key), value);
		}

		for (Map.Entry<String, byte[]> entry : values.entrySet())
		{
			RecordEntry entry1 = new RecordEntry(entry.getKey().getBytes());
			assertTrue(map.get(entry1));
			assertEquals(entry1.getValue(), entry.getValue());
		}
	}


	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testExceedBufferSize()
	{
		ArrayMap map = new ArrayMap(65536);

		HashMap<byte[], byte[]> values = new HashMap<>();

		byte[] key = createRandomBuffer(0, 32767);
		byte[] value = createRandomBuffer(1, 32759);

		map.put(new RecordEntry(key, value, (byte)0));

		values.put(key, value);

		fail();
	}


	@Test
	public void testReplaceEntry()
	{
		byte[][] keys = new byte[1000][];
		for (int i = 0; i < 1000; i++)
		{
			keys[i] = tb();
		}

		ArrayMap map = new ArrayMap(4096);

		HashMap<String, byte[]> values = new HashMap<>();

		for (int i = 0; i < 100000; i++)
		{
			int j = c() % keys.length;
			byte[] value = tb();
			byte[] key = keys[j];

			if (map.put(new RecordEntry(key, value, (byte)0)))
			{
				values.put(new String(key), value);
			}
		}

		assertNull(map.integrityCheck());

		for (Map.Entry<String, byte[]> entry : values.entrySet())
		{
			RecordEntry entry1 = new RecordEntry(entry.getKey().getBytes());
			assertTrue(map.get(entry1));
			assertEquals(entry1.getValue(), entry.getValue());
		}
	}


	@Test
	public void testNearest() throws IOException
	{
		byte[] b = "123".getBytes();
		byte[] d = "456".getBytes();

		ArrayMap map = new ArrayMap(new byte[512]);
		map.put(new RecordEntry("b".getBytes(), b, (byte)0));
		map.put(new RecordEntry("d".getBytes(), d, (byte)0));

		RecordEntry A = new RecordEntry("a".getBytes());
		RecordEntry B = new RecordEntry("b".getBytes());
		RecordEntry C = new RecordEntry("c".getBytes());
		RecordEntry D = new RecordEntry("d".getBytes());
		RecordEntry E = new RecordEntry("e".getBytes());

		assertEquals(map.nearest(A), ArrayMap.NEAR);
		assertEquals(A.getValue(), b);

		assertEquals(map.nearest(B), ArrayMap.EXACT);
		assertEquals(B.getValue(), b);

		assertEquals(map.nearest(C), ArrayMap.NEAR);
		assertEquals(C.getValue(), d);

		assertEquals(map.nearest(D), ArrayMap.EXACT);
		assertEquals(D.getValue(), d);

		assertEquals(map.nearest(E), ArrayMap.LAST);
	}
}
