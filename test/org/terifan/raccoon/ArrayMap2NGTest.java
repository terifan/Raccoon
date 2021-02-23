package org.terifan.raccoon;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.terifan.raccoon.util.Result;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import static resources.__TestUtils.*;


public class ArrayMap2NGTest
{
	public ArrayMap2NGTest()
	{
		ArrayMap2.class.getClassLoader().setClassAssertionStatus(ArrayMap2.class.getName(), false);
	}


	@Test
	public void testPutGet()
	{
		ArrayMap2 map = new ArrayMap2(1000_000);

		byte[] key = tb();
		byte[] value = tb();

		assertTrue(map.put(new ArrayMapEntry2(key, value, (byte)77), null));

		ArrayMapEntry2 entry = new ArrayMapEntry2(key);

		assertTrue(map.get(entry));
		assertEquals(entry.getValue(), value);
	}


	@Test
	public void testPutRemove()
	{
		ArrayMap2 map = new ArrayMap2(1000_000);

		byte[] key = tb();
		byte[] value = tb();

		ArrayMapEntry2 entry = new ArrayMapEntry2(key, value, (byte)77);

		assertTrue(map.put(entry, null));

		Result<ArrayMapEntry2> oldEntry = new Result<>();

		assertTrue(map.remove(entry, oldEntry));

		assertEquals(entry.getValue(), oldEntry.get().getValue());
	}


	@Test
	public void testRemoveNonExisting()
	{
		ArrayMap2 map = new ArrayMap2(1000_000);

		byte[] key = tb();

		ArrayMapEntry2 entry = new ArrayMapEntry2(key);

		Result<ArrayMapEntry2> oldEntry = new Result<>();

		assertFalse(map.remove(entry, oldEntry));

		assertNull(oldEntry.get());
	}


	@Test
	public void testFillBuffer()
	{
		ArrayMap2 map = new ArrayMap2(1000_000);

		HashMap<String, byte[]> expected = new HashMap<>();

		fillArrayMap(map, expected);

		for (Map.Entry<String, byte[]> expectedEntry : expected.entrySet())
		{
			ArrayMapEntry2 entry = new ArrayMapEntry2(expectedEntry.getKey().getBytes());
			assertTrue(map.get(entry));
			assertEquals(entry.getValue(), expectedEntry.getValue());
		}
	}


	@Test
	public void testIterator()
	{
		ArrayMap2 map = new ArrayMap2(1000_000);

		HashMap<String, byte[]> expected = new HashMap<>();

		fillArrayMap(map, expected);

		HashSet<String> found = new HashSet<>();

		for (ArrayMapEntry2 entry : map)
		{
			String k = new String(entry.getKey());
			assertEquals(entry.getValue(), expected.get(k));
			found.add(k);
		}

		assertEquals(found, expected.keySet());
	}


	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testExceedBufferSize()
	{
		ArrayMap2 map = new ArrayMap2(1000_000);

		HashMap<byte[], byte[]> values = new HashMap<>();

		int n = map.getCapacity() - 2-4 - 2-2 - 4;

		byte[] key = createRandomBuffer(0, n / 2);
		byte[] value = createRandomBuffer(1, n - n / 2 + 1); // one byte exceeding maximum size

		map.put(new ArrayMapEntry2(key, value, (byte)77), null);

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

		ArrayMap2 map = new ArrayMap2(1000_000);

		HashMap<String, byte[]> values = new HashMap<>();

		for (int i = 0; i < 100000; i++)
		{
			int j = c() % keys.length;
			byte[] value = tb();
			byte[] key = keys[j];

			if (map.put(new ArrayMapEntry2(key, value, (byte)77), null))
			{
				values.put(new String(key), value);
			}
		}

		assertNull(map.integrityCheck());

		for (Map.Entry<String, byte[]> entry : values.entrySet())
		{
			ArrayMapEntry2 entry1 = new ArrayMapEntry2(entry.getKey().getBytes());
			assertTrue(map.get(entry1));
			assertEquals(entry1.getValue(), entry.getValue());
		}
	}


	@Test
	public void testNearest() throws IOException
	{
		byte[] b = "123".getBytes();
		byte[] d = "456".getBytes();

		ArrayMap2 map = new ArrayMap2(new byte[512]);
		map.put(new ArrayMapEntry2("b".getBytes(), b, (byte)77), null);
		map.put(new ArrayMapEntry2("d".getBytes(), d, (byte)77), null);

		ArrayMapEntry2 A = new ArrayMapEntry2("a".getBytes());
		ArrayMapEntry2 B = new ArrayMapEntry2("b".getBytes());
		ArrayMapEntry2 C = new ArrayMapEntry2("c".getBytes());
		ArrayMapEntry2 D = new ArrayMapEntry2("d".getBytes());
		ArrayMapEntry2 E = new ArrayMapEntry2("e".getBytes());

		assertEquals(map.nearest(A), ArrayMap2.NEAR);
		assertEquals(A.getValue(), b);

		assertEquals(map.nearest(B), ArrayMap2.EXACT);
		assertEquals(B.getValue(), b);

		assertEquals(map.nearest(C), ArrayMap2.NEAR);
		assertEquals(C.getValue(), d);

		assertEquals(map.nearest(D), ArrayMap2.EXACT);
		assertEquals(D.getValue(), d);

		assertEquals(map.nearest(E), ArrayMap2.LAST);
	}


	private void fillArrayMap(ArrayMap2 aMap, HashMap<String, byte[]> aValues)
	{
		for (;;)
		{
			byte[] key = tb();
			byte[] value = tb();

			if (!aMap.put(new ArrayMapEntry2(key, value, (byte)77), null))
			{
				break;
			}

			aValues.put(new String(key), value);
		}
	}
}
