package org.terifan.raccoon;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.terifan.raccoon.util.Result;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import static resources.__TestUtils.*;


public class ArrayMap2NGTest
{
	@Test
	public void testPutGet()
	{
		ArrayMap2 map = new ArrayMap2(4096);

		byte[] key = tb();
		byte[] value = tb();

		assertTrue(map.put(new ArrayMapEntry2(key, value), null));

		ArrayMapEntry2 entry = new ArrayMapEntry2(key);

		assertTrue(map.get(entry));
		assertEquals(getValue(entry), value);
	}


	@Test
	public void testPutRemove()
	{
		ArrayMap2 map = new ArrayMap2(4096);

		byte[] key = tb();
		byte[] value = tb();

		ArrayMapEntry2 entry = new ArrayMapEntry2(key, value);

		assertTrue(map.put(entry, null));

		Result<ArrayMapEntry2> oldEntry = new Result<>();

		assertTrue(map.remove(entry, oldEntry));

		assertEquals(getValue(entry), getValue(oldEntry.get()));
	}


	@Test
	public void testRemoveNonExisting()
	{
		ArrayMap2 map = new ArrayMap2(4096);

		byte[] key = tb();

		ArrayMapEntry2 entry = new ArrayMapEntry2(key, null);

		Result<ArrayMapEntry2> oldEntry = new Result<>();

		assertFalse(map.remove(entry, oldEntry));

		assertNull(oldEntry.get());
	}


	@Test
	public void testFillBuffer()
	{
		ArrayMap2 map = new ArrayMap2(250_000);

		HashMap<String, byte[]> values = new HashMap<>();

		for (;;)
		{
			byte[] key = tb();
			byte[] value = tb();

			if (!map.put(new ArrayMapEntry2(key, value), null))
			{
				break;
			}

			values.put(new String(key), value);
		}

		for (Map.Entry<String, byte[]> expected : values.entrySet())
		{
			ArrayMapEntry2 entry = new ArrayMapEntry2(expected.getKey().getBytes());
			assertTrue(map.get(entry));
			assertEquals(getValue(entry), expected.getValue());
		}
	}


	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testExceedBufferSize()
	{
		ArrayMap2 map = new ArrayMap2(250_000);

		HashMap<byte[], byte[]> values = new HashMap<>();

		int n = map.getCapacity() - 2-4 - 2-2 - 4;

		byte[] key = createRandomBuffer(0, n / 2);
		byte[] value = createRandomBuffer(1, n - n / 2 + 1); // one byte exceeding maximum size

		map.put(new ArrayMapEntry2(key, value), null);

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

		ArrayMap2 map = new ArrayMap2(4096);

		HashMap<String, byte[]> values = new HashMap<>();

		for (int i = 0; i < 100000; i++)
		{
			int j = c() % keys.length;
			byte[] value = tb();
			byte[] key = keys[j];

			if (map.put(new ArrayMapEntry2(key, value), null))
			{
				values.put(new String(key), value);
			}
		}

		assertNull(map.integrityCheck());

		for (Map.Entry<String, byte[]> entry : values.entrySet())
		{
			ArrayMapEntry2 entry1 = new ArrayMapEntry2(entry.getKey().getBytes());
			assertTrue(map.get(entry1));
			assertEquals(getValue(entry1), entry.getValue());
		}
	}


	@Test
	public void testNearest() throws IOException
	{
		byte[] b = "123".getBytes();
		byte[] d = "456".getBytes();

		ArrayMap2 map = new ArrayMap2(new byte[512]);
		map.put(new ArrayMapEntry2("b".getBytes(), b), null);
		map.put(new ArrayMapEntry2("d".getBytes(), d), null);

		ArrayMapEntry2 A = new ArrayMapEntry2("a".getBytes());
		ArrayMapEntry2 B = new ArrayMapEntry2("b".getBytes());
		ArrayMapEntry2 C = new ArrayMapEntry2("c".getBytes());
		ArrayMapEntry2 D = new ArrayMapEntry2("d".getBytes());
		ArrayMapEntry2 E = new ArrayMapEntry2("e".getBytes());

		assertEquals(map.nearest(A), ArrayMap2.NEAR);
		assertEquals(getValue(A), b);

		assertEquals(map.nearest(B), ArrayMap2.EXACT);
		assertEquals(getValue(B), b);

		assertEquals(map.nearest(C), ArrayMap2.NEAR);
		assertEquals(getValue(C), d);

		assertEquals(map.nearest(D), ArrayMap2.EXACT);
		assertEquals(getValue(D), d);

		assertEquals(map.nearest(E), ArrayMap2.LAST);
	}


	private Object getValue(ArrayMapEntry2 aEntry)
	{
		return aEntry.getValue(new byte[aEntry.getValueLength()], 0);
	}
}
