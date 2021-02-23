package org.terifan.raccoon;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.terifan.raccoon.util.Result;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import static resources.__TestUtils.*;


public class ArrayMapNGTest
{
	public ArrayMapNGTest()
	{
		ArrayMap.class.getClassLoader().setClassAssertionStatus(ArrayMap.class.getName(), false);
	}


	@Test
	public void testPutGet()
	{
		ArrayMap map = new ArrayMap(1000_000);

		byte[] key = tb();
		byte[] value = tb();
		byte flags = (byte)77;

		assertTrue(map.put(new ArrayMapEntry(key, value, flags), null));

		ArrayMapEntry entry = new ArrayMapEntry(key);

		assertTrue(map.get(entry));
		assertEquals(entry.getValue(), value);
		assertEquals(entry.getFlags(), flags);
	}


	@Test
	public void testPutRemove()
	{
		ArrayMap map = new ArrayMap(1000_000);

		byte[] key = tb();
		byte[] value = tb();
		byte flags = (byte)77;

		ArrayMapEntry entry = new ArrayMapEntry(key, value, flags);

		assertTrue(map.put(entry, null));

		Result<ArrayMapEntry> oldEntry = new Result<>();

		assertTrue(map.remove(entry, oldEntry));

		assertEquals(entry.getValue(), oldEntry.get().getValue());
	}


	@Test
	public void testRemoveNonExisting()
	{
		ArrayMap map = new ArrayMap(1000_000);

		byte[] key = tb();

		ArrayMapEntry entry = new ArrayMapEntry(key);

		Result<ArrayMapEntry> oldEntry = new Result<>();

		assertFalse(map.remove(entry, oldEntry));

		assertNull(oldEntry.get());
	}


	@Test
	public void testFillBuffer()
	{
		ArrayMap map = new ArrayMap(1000_000);

		HashMap<String, byte[]> expected = new HashMap<>();

		fillArrayMap(map, expected);

		for (Map.Entry<String, byte[]> expectedEntry : expected.entrySet())
		{
			ArrayMapEntry entry = new ArrayMapEntry(expectedEntry.getKey().getBytes());
			assertTrue(map.get(entry));
			assertEquals(entry.getValue(), expectedEntry.getValue());
		}
	}


	@Test
	public void testMaxEntriesOverflow()
	{
		ArrayMap map = new ArrayMap(1000_000);

		byte[] value = new byte[0];

		for (int i = 0; i < ArrayMap.MAX_ENTRY_COUNT; i++)
		{
			byte[] key = ("" + i).getBytes();

			assertTrue(map.put(new ArrayMapEntry(key, value, (byte)77), null));
		}

		byte[] key = ("" + (ArrayMap.MAX_ENTRY_COUNT + 1)).getBytes();

		assertFalse(map.put(new ArrayMapEntry(key, value, (byte)77), null));
	}


	@Test
	public void testIterator()
	{
		ArrayMap map = new ArrayMap(1000_000);

		HashMap<String, byte[]> expected = new HashMap<>();

		fillArrayMap(map, expected);

		HashSet<String> found = new HashSet<>();

		for (ArrayMapEntry entry : map)
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
		ArrayMap map = new ArrayMap(1000_000);

		HashMap<byte[], byte[]> values = new HashMap<>();

		int n = map.getCapacity() - 2-4 - 2-2 - 4;

		byte[] key = createRandomBuffer(0, n / 2);
		byte[] value = createRandomBuffer(1, n - n / 2 + 1); // one byte exceeding maximum size
		byte flags = (byte)77;

		map.put(new ArrayMapEntry(key, value, flags), null);

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

		ArrayMap map = new ArrayMap(1000_000);

		HashMap<String, byte[]> values = new HashMap<>();

		byte flags = (byte)77;

		for (int i = 0; i < 100000; i++)
		{
			int j = c() % keys.length;
			byte[] value = tb();
			byte[] key = keys[j];

			if (map.put(new ArrayMapEntry(key, value, flags), null))
			{
				values.put(new String(key), value);
			}
		}

		assertNull(map.integrityCheck());

		for (Map.Entry<String, byte[]> entry : values.entrySet())
		{
			ArrayMapEntry entry1 = new ArrayMapEntry(entry.getKey().getBytes());
			assertTrue(map.get(entry1));
			assertEquals(entry1.getValue(), entry.getValue());
			assertEquals(entry1.getFlags(), flags);
		}
	}


	@Test
	public void testNearest() throws IOException
	{
		byte[] b = "123".getBytes();
		byte[] d = "456".getBytes();

		ArrayMap map = new ArrayMap(new byte[512]);
		map.put(new ArrayMapEntry("b".getBytes(), b, (byte)77), null);
		map.put(new ArrayMapEntry("d".getBytes(), d, (byte)77), null);

		ArrayMapEntry A = new ArrayMapEntry("a".getBytes());
		ArrayMapEntry B = new ArrayMapEntry("b".getBytes());
		ArrayMapEntry C = new ArrayMapEntry("c".getBytes());
		ArrayMapEntry D = new ArrayMapEntry("d".getBytes());
		ArrayMapEntry E = new ArrayMapEntry("e".getBytes());

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


	private void fillArrayMap(ArrayMap aMap, HashMap<String, byte[]> aValues)
	{
		for (;;)
		{
			byte[] key = tb();
			byte[] value = tb();
			byte flags = (byte)77;

			if (!aMap.put(new ArrayMapEntry(key, value, flags), null))
			{
				break;
			}

			aValues.put(new String(key), value);
		}
	}
}
