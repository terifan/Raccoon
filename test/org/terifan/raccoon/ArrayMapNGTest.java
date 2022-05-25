package org.terifan.raccoon;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.terifan.raccoon.ArrayMap.NearResult;
import org.terifan.raccoon.util.Log;
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
	public void testPutGetReplaceRemove()
	{
		ArrayMap map = new ArrayMap(100);

		byte[] original = "green".getBytes();
		ArrayMapEntry out = new ArrayMapEntry("apple".getBytes(), original, (byte)'-');

		Result<ArrayMapEntry> existing = new Result<>();

		boolean wasAdd = map.put(out, existing);

		assertTrue(wasAdd);
		assertNull(existing.get());

		ArrayMapEntry in = new ArrayMapEntry(out.getKey());

		boolean wasFound = map.get(in);

		assertTrue(wasFound);
		assertEquals(in.getValue(), out.getValue());
		assertEquals(in.getType(), out.getType());
		assertEquals(map.getFreeSpace(), 75);

		out.setValue("red".getBytes());

		wasAdd = map.put(out, existing);

		assertTrue(wasAdd);
		assertNotNull(existing.get());
		assertEquals(existing.get().getValue(), original);

		in = new ArrayMapEntry(out.getKey());

		wasFound = map.get(in);

		assertTrue(wasFound);
		assertEquals(in.getValue(), out.getValue());
		assertEquals(in.getType(), out.getType());
		assertEquals(map.getFreeSpace(), 77);

		wasFound = map.remove(in, existing);

		assertTrue(wasFound);
		assertEquals(existing.get().getValue(), out.getValue());
		assertEquals(existing.get().getType(), out.getType());
		assertEquals(map.getFreeSpace(), 94);

		wasFound = map.remove(in, existing);

		assertFalse(wasFound);
		assertNull(existing.get());
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
	public void testFillBuffer() throws UnsupportedEncodingException
	{
		ArrayMap map = new ArrayMap(1000_000);

		HashMap<String, byte[]> expected = new HashMap<>();

		fillArrayMap(map, expected);

		for (Map.Entry<String, byte[]> expectedEntry : expected.entrySet())
		{
			ArrayMapEntry entry = new ArrayMapEntry(expectedEntry.getKey().getBytes("utf-8"));

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

		byte[] key = ("" + ArrayMap.MAX_ENTRY_COUNT).getBytes();

		assertFalse(map.put(new ArrayMapEntry(key, value, (byte)77), null));
	}


	@Test
	public void testIterator() throws UnsupportedEncodingException
	{
		ArrayMap map = new ArrayMap(1000_000);

		HashMap<String, byte[]> expected = new HashMap<>();

		fillArrayMap(map, expected);

		HashSet<String> found = new HashSet<>();

		for (ArrayMapEntry entry : map)
		{
			String k = new String(entry.getKey(), "utf-8");
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
	public void testReplaceEntry() throws UnsupportedEncodingException
	{
		String[] keys = new String[1000];
		for (int i = 0; i < keys.length; i++)
		{
			keys[i] = new String(tb(), "utf-8");
		}

		ArrayMap map = new ArrayMap(1000_000);

		HashMap<String, byte[]> values = new HashMap<>();

		byte type = (byte)77;

		for (int i = 0; i < 100_000; i++)
		{
			int j = c() % keys.length;
			byte[] value = tb();
			String keyString = keys[j];
			byte[] key = keyString.getBytes("utf-8");

			if (map.put(new ArrayMapEntry(key, value, type), null))
			{
				values.put(keyString, value);
			}
		}

		assertNull(map.integrityCheck());

		for (Map.Entry<String, byte[]> entry : values.entrySet())
		{
			ArrayMapEntry entry1 = new ArrayMapEntry(entry.getKey().getBytes("utf-8"));
			assertTrue(map.get(entry1));
			assertEquals(entry1.getValue(), entry.getValue());
			assertEquals(entry1.getType(), type);
		}
	}


	@Test
	public void testNearestA() throws IOException
	{
		byte[] value1 = "123".getBytes();
		byte[] value2 = "456".getBytes();

		ArrayMap map = new ArrayMap(new byte[512]);
		map.put(new ArrayMapEntry("b".getBytes(), value1, (byte)77), null);
		map.put(new ArrayMapEntry("d".getBytes(), value2, (byte)77), null);

		ArrayMapEntry A = new ArrayMapEntry("a".getBytes());
		ArrayMapEntry B = new ArrayMapEntry("b".getBytes());
		ArrayMapEntry C = new ArrayMapEntry("c".getBytes());
		ArrayMapEntry D = new ArrayMapEntry("d".getBytes());
		ArrayMapEntry E = new ArrayMapEntry("e".getBytes());

		assertEquals(map.nearest(A), NearResult.LOWER); // a is lower than b
		assertEquals(A.getValue(), value1);

		assertEquals(map.nearest(B), NearResult.MATCH); // b matches
		assertEquals(B.getValue(), value1);

		assertEquals(map.nearest(C), NearResult.LOWER); // c is lower than d
		assertEquals(C.getValue(), value2);

		assertEquals(map.nearest(D), NearResult.MATCH); // d matches
		assertEquals(D.getValue(), value2);

		assertEquals(map.nearest(E), NearResult.GREATER); // e is greater
	}


	@Test
	public void testNearestB() throws IOException
	{
		byte[] value1 = "123".getBytes();
		byte[] value2 = "456".getBytes();

		ArrayMap map = new ArrayMap(new byte[512]);
		map.put(new ArrayMapEntry("bbb".getBytes(), value1, (byte)77), null);
		map.put(new ArrayMapEntry("dd".getBytes(), value2, (byte)77), null);

		ArrayMapEntry A = new ArrayMapEntry("aaaaa".getBytes());
		ArrayMapEntry B = new ArrayMapEntry("bbb".getBytes());
		ArrayMapEntry C = new ArrayMapEntry("c".getBytes());
		ArrayMapEntry D = new ArrayMapEntry("dd".getBytes());
		ArrayMapEntry E = new ArrayMapEntry("eeee".getBytes());

		assertEquals(map.nearest(A), NearResult.LOWER); // a is lower than b
		assertEquals(A.getValue(), value1);

		assertEquals(map.nearest(B), NearResult.MATCH); // b matches
		assertEquals(B.getValue(), value1);

		assertEquals(map.nearest(C), NearResult.LOWER); // c is lower than d
		assertEquals(C.getValue(), value2);

		assertEquals(map.nearest(D), NearResult.MATCH); // d matches
		assertEquals(D.getValue(), value2);

		assertEquals(map.nearest(E), NearResult.GREATER); // e is last
	}


	@Test
	public void testNearest2A() throws IOException
	{
		byte[] value1 = "123".getBytes();
		byte[] value2 = "456".getBytes();
		byte[] value3 = "789".getBytes();

		ArrayMap map = new ArrayMap(new byte[512]);
		map.put(new ArrayMapEntry("".getBytes(), value1, (byte)77), null);
		map.put(new ArrayMapEntry("b".getBytes(), value2, (byte)77), null);
		map.put(new ArrayMapEntry("d".getBytes(), value3, (byte)77), null);

		ArrayMapEntry A = new ArrayMapEntry("a".getBytes());
		ArrayMapEntry B = new ArrayMapEntry("b".getBytes());
		ArrayMapEntry C = new ArrayMapEntry("c".getBytes());
		ArrayMapEntry D = new ArrayMapEntry("d".getBytes());
		ArrayMapEntry E = new ArrayMapEntry("e".getBytes());

		assertEquals(map.nearestIndexEntry(A), NearResult.LOWER); // a is lower than b
		assertEquals(A.getValue(), value1);

		assertEquals(map.nearestIndexEntry(B), NearResult.MATCH); // b matches
		assertEquals(B.getValue(), value2);

		assertEquals(map.nearestIndexEntry(C), NearResult.LOWER); // c is lower than d
		assertEquals(C.getValue(), value2);

		assertEquals(map.nearestIndexEntry(D), NearResult.MATCH); // d matches
		assertEquals(D.getValue(), value3);

		assertEquals(map.nearestIndexEntry(E), NearResult.GREATER); // e is last
		assertEquals(E.getValue(), value3);
	}


	@Test
	public void testKeyOrder() throws IOException
	{
		byte[] value = "123".getBytes();

		ArrayMap map = new ArrayMap(new byte[512]);
		map.put(new ArrayMapEntry("eeee".getBytes(), value, (byte)77), null);
		map.put(new ArrayMapEntry("c".getBytes(), value, (byte)77), null);
		map.put(new ArrayMapEntry("aaaaa".getBytes(), value, (byte)77), null);
		map.put(new ArrayMapEntry("dd".getBytes(), value, (byte)77), null);
		map.put(new ArrayMapEntry("bbb".getBytes(), value, (byte)77), null);
		map.put(new ArrayMapEntry("ddd".getBytes(), value, (byte)77), null);

		assertEquals(map.toString(), "{\"aaaaa\",\"bbb\",\"c\",\"dd\",\"ddd\",\"eeee\"}");
	}


	public static void fillArrayMap(ArrayMap aMap, HashMap<String, byte[]> aValues)
	{
		try
		{
			for (;;)
			{
				String keyString = new String(tb(), "utf-8");
				byte[] key = keyString.getBytes("utf-8");
				byte[] value = tb();
				byte type = b();

				ArrayMapEntry entry = new ArrayMapEntry(key, value, type);

				if (entry.getMarshalledLength() > aMap.getCapacity() - ArrayMap.HEADER_SIZE - ArrayMap.ENTRY_HEADER_SIZE - ArrayMap.ENTRY_POINTER_SIZE)
				{
					continue;
				}

				if (!aMap.put(entry, null))
				{
					break;
				}

				aValues.put(keyString, value);
			}
		}
		catch (UnsupportedEncodingException e)
		{
			throw new IllegalStateException(e);
		}
	}
}
