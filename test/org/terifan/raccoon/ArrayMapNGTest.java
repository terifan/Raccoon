package org.terifan.raccoon;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import org.terifan.raccoon.ArrayMap.NearResult;
import org.terifan.raccoon.ArrayMap.PutResult;
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
		ArrayMapEntry out = new ArrayMapEntry(new ArrayMapKey("apple"), original, (byte)'-');

		Result<ArrayMapEntry> existing = new Result<>();

		PutResult wasAdd = map.put(out, existing);

		assertNotEquals(wasAdd, PutResult.OVERFLOW);
		assertNull(existing.get());

		ArrayMapEntry in = new ArrayMapEntry(out.getKey());

		boolean wasFound = map.get(in);

		assertTrue(wasFound);
		assertEquals(in.getValue(), out.getValue());
		assertEquals(in.getType(), out.getType());
		assertEquals(map.getFreeSpace(), 75);

		out.setValue("red".getBytes());

		wasAdd = map.put(out, existing);

		assertNotEquals(wasAdd, PutResult.OVERFLOW);
		assertNotNull(existing.get());
		assertEquals(existing.get().getValue(), original);

		in = new ArrayMapEntry(out.getKey());

		wasFound = map.get(in);

		assertTrue(wasFound);
		assertEquals(in.getValue(), out.getValue());
		assertEquals(in.getType(), out.getType());
		assertEquals(map.getFreeSpace(), 77);

		wasFound = map.remove(in.getKey(), existing);

		assertTrue(wasFound);
		assertEquals(existing.get().getValue(), out.getValue());
		assertEquals(existing.get().getType(), out.getType());
		assertEquals(map.getFreeSpace(), 94);

		wasFound = map.remove(in.getKey(), existing);

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

		ArrayMapEntry entry = new ArrayMapEntry(new ArrayMapKey(key), value, flags);

		assertNotEquals(map.put(entry, null), PutResult.OVERFLOW);

		Result<ArrayMapEntry> oldEntry = new Result<>();

		assertTrue(map.remove(entry.getKey(), oldEntry));

		assertEquals(entry.getValue(), oldEntry.get().getValue());
	}


	@Test
	public void testRemoveNonExisting()
	{
		ArrayMap map = new ArrayMap(1000_000);

		byte[] key = tb();

		ArrayMapEntry entry = new ArrayMapEntry(new ArrayMapKey(key));

		Result<ArrayMapEntry> oldEntry = new Result<>();

		assertFalse(map.remove(entry.getKey(), oldEntry));

		assertNull(oldEntry.get());
	}


	@Test
	public void testFillBuffer() throws UnsupportedEncodingException
	{
		ArrayMap map = new ArrayMap(1000_000);

		HashMap<ArrayMapKey, byte[]> expected = new HashMap<>();

		fillArrayMap(map, expected);

		for (Entry<ArrayMapKey, byte[]> expectedEntry : expected.entrySet())
		{
			ArrayMapEntry entry = new ArrayMapEntry(expectedEntry.getKey());

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
			ArrayMapKey key = new ArrayMapKey("" + i);

			assertNotEquals(map.put(new ArrayMapEntry(key, value, (byte)77), null), PutResult.OVERFLOW);
		}

		ArrayMapKey key = new ArrayMapKey("" + ArrayMap.MAX_ENTRY_COUNT);

		assertEquals(map.put(new ArrayMapEntry(key, value, (byte)77), null), PutResult.OVERFLOW);
	}


	@Test
	public void testIterator() throws UnsupportedEncodingException
	{
		ArrayMap map = new ArrayMap(1000_000);

		HashMap<ArrayMapKey, byte[]> expected = new HashMap<>();

		fillArrayMap(map, expected);

		HashSet<ArrayMapKey> found = new HashSet<>();

		for (ArrayMapEntry entry : map)
		{
			ArrayMapKey k = entry.getKey();
			assertEquals(entry.getValue(), expected.get(k));
			found.add(k);
		}

		assertEquals(found, expected.keySet());
	}


	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testExceedBufferSize()
	{
		ArrayMap map = new ArrayMap(1000_000);

		HashMap<ArrayMapKey, byte[]> values = new HashMap<>();

		int n = map.getCapacity() - 2-4 - 2-2 - 4;

		ArrayMapKey key = new ArrayMapKey(createRandomBuffer(0, n / 2));
		byte[] value = createRandomBuffer(1, n - n / 2 + 1); // one byte exceeding maximum size
		byte flags = (byte)77;

		map.put(new ArrayMapEntry(key, value, flags), null);

		values.put(key, value);

		fail();
	}


	@Test
	public void testReplaceEntry() throws UnsupportedEncodingException
	{
		ArrayMapKey[] keys = new ArrayMapKey[1000];
		for (int i = 0; i < keys.length; i++)
		{
			keys[i] = new ArrayMapKey(tb());
		}

		ArrayMap map = new ArrayMap(1000_000);

		HashMap<ArrayMapKey, byte[]> values = new HashMap<>();

		byte type = (byte)77;

		for (int i = 0; i < 100_000; i++)
		{
			int j = c() % keys.length;
			byte[] value = tb();
			ArrayMapKey key = keys[j];

			if (map.put(new ArrayMapEntry(key, value, type), null) != PutResult.OVERFLOW)
			{
				values.put(key, value);
			}
		}

		assertNull(map.integrityCheck());

		for (Entry<ArrayMapKey, byte[]> entry : values.entrySet())
		{
			ArrayMapEntry entry1 = new ArrayMapEntry(entry.getKey());
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
		map.put(new ArrayMapEntry(new ArrayMapKey("b"), value1, (byte)77), null);
		map.put(new ArrayMapEntry(new ArrayMapKey("d"), value2, (byte)77), null);

		ArrayMapEntry A = new ArrayMapEntry(new ArrayMapKey("a"));
		ArrayMapEntry B = new ArrayMapEntry(new ArrayMapKey("b"));
		ArrayMapEntry C = new ArrayMapEntry(new ArrayMapKey("c"));
		ArrayMapEntry D = new ArrayMapEntry(new ArrayMapKey("d"));
		ArrayMapEntry E = new ArrayMapEntry(new ArrayMapKey("e"));

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
		map.put(new ArrayMapEntry(new ArrayMapKey("bbb"), value1, (byte)77), null);
		map.put(new ArrayMapEntry(new ArrayMapKey("dd"), value2, (byte)77), null);

		ArrayMapEntry A = new ArrayMapEntry(new ArrayMapKey("aaaaa"));
		ArrayMapEntry B = new ArrayMapEntry(new ArrayMapKey("bbb"));
		ArrayMapEntry C = new ArrayMapEntry(new ArrayMapKey("c"));
		ArrayMapEntry D = new ArrayMapEntry(new ArrayMapKey("dd"));
		ArrayMapEntry E = new ArrayMapEntry(new ArrayMapKey("eeee"));

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
	public void testKeyOrder() throws IOException
	{
		byte[] value = "123".getBytes();

		ArrayMap map = new ArrayMap(new byte[512]);
		map.put(new ArrayMapEntry(new ArrayMapKey("eeee"), value, (byte)77), null);
		map.put(new ArrayMapEntry(new ArrayMapKey("c"), value, (byte)77), null);
		map.put(new ArrayMapEntry(new ArrayMapKey("aaaaa"), value, (byte)77), null);
		map.put(new ArrayMapEntry(new ArrayMapKey("dd"), value, (byte)77), null);
		map.put(new ArrayMapEntry(new ArrayMapKey("bbb"), value, (byte)77), null);
		map.put(new ArrayMapEntry(new ArrayMapKey("ddd"), value, (byte)77), null);

		assertEquals(map.toString(), "[\"aaaaa\",\"bbb\",\"c\",\"dd\",\"ddd\",\"eeee\"]");
	}


	public static void fillArrayMap(ArrayMap aMap, HashMap<ArrayMapKey, byte[]> aValues)
	{
		for (;;)
		{
			ArrayMapKey key = new ArrayMapKey(tb());
			byte[] value = tb();
			byte type = b();

			ArrayMapEntry entry = new ArrayMapEntry(key, value, type);

			if (entry.getMarshalledLength() > aMap.getCapacity() - ArrayMap.HEADER_SIZE - ArrayMap.ENTRY_HEADER_SIZE - ArrayMap.ENTRY_POINTER_SIZE)
			{
				continue;
			}

			if (aMap.put(entry, null) == PutResult.OVERFLOW)
			{
				break;
			}

			aValues.put(key, value);
		}
	}
}
