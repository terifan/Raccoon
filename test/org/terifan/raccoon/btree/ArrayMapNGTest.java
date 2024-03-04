package org.terifan.raccoon.btree;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import org.terifan.raccoon.document.Document;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import static org.terifan.raccoon._Tools.b;
import static org.terifan.raccoon._Tools.c;
import static org.terifan.raccoon._Tools.doc;
import static org.terifan.raccoon._Tools.t;


public class ArrayMapNGTest
{
	public ArrayMapNGTest()
	{
		ArrayMap.class.getClassLoader().setClassAssertionStatus(ArrayMap.class.getName(), false);
	}


	@Test
	public void testPutGetReplaceRemove()
	{
		ArrayMap map = new ArrayMap(100, 1);

		Document original = doc();
		ArrayMapEntry out = new ArrayMapEntry(new ArrayMapKey("apple"), original, (byte)'-');

		OpResult wasAdd = map.put(out);

		assertNotEquals(wasAdd, OpState.OVERFLOW);
		assertNull(wasAdd.entry);

		OpResult existing = map.get(out.getKey());

		assertEquals(existing.state, OpState.MATCH);
		assertEquals(existing.entry.getValue(), out.getValue());
		assertEquals(existing.entry.getType(), out.getType());
//		assertEquals(map.getFreeSpace(), 73);

		out.setValue(doc());

		wasAdd = map.put(out);

		assertNotEquals(wasAdd, OpState.OVERFLOW);
		assertNotNull(wasAdd.entry);
		assertEquals(wasAdd.entry.getValue(), original);

		existing = map.get(out.getKey());

		assertEquals(existing.state, OpState.MATCH);
		assertEquals(existing.entry.getValue(), out.getValue());
		assertEquals(existing.entry.getType(), out.getType());
//		assertEquals(map.getFreeSpace(), 77);

		existing = map.remove(out.getKey());

		assertEquals(existing.state, OpState.MATCH);
		assertEquals(existing.entry.getValue(), out.getValue());
		assertEquals(existing.entry.getType(), out.getType());
//		assertEquals(map.getFreeSpace(), 94);

		existing = map.remove(out.getKey());

		assertEquals(existing.state, OpState.NO_MATCH);
		assertNull(existing.entry);
	}


	@Test
	public void testPutRemove()
	{
		ArrayMap map = new ArrayMap(1000_000, 1);

		String key = t();
		Document value = doc();
		byte flags = (byte)77;

		ArrayMapEntry entry = new ArrayMapEntry(new ArrayMapKey(key), value, flags);

		OpResult op = map.put(entry);

		assertNotEquals(op.state, OpState.OVERFLOW);

		op = map.remove(entry.getKey());

		assertEquals(op.state, OpState.DELETE);

//		assertEquals(entry, op.entry);
	}


	@Test
	public void testRemoveNonExisting()
	{
		ArrayMap map = new ArrayMap(1000_000, 1);

		String key = t();

		ArrayMapEntry entry = new ArrayMapEntry(new ArrayMapKey(key));

		OpResult old = map.remove(entry.getKey());

		assertEquals(old.state, OpState.NO_MATCH);
		assertNull(old.entry);
	}


	@Test
	public void testFillBuffer() throws UnsupportedEncodingException
	{
		ArrayMap map = new ArrayMap(1000_000, 1);

		HashMap<ArrayMapKey, Document> expected = new HashMap<>();

		fillArrayMap(map, expected);

		for (Entry<ArrayMapKey, Document> expectedEntry : expected.entrySet())
		{
			OpResult entry = map.get(expectedEntry.getKey());

			assertEquals(entry.state, OpState.MATCH);
			assertEquals(entry.entry.getValue(), expectedEntry.getValue());
		}
	}


//	@Test
//	public void testMaxEntriesOverflow()
//	{
//		ArrayMap map = new ArrayMap(2_000_000);
//
//		Document value = doc(1);
//
//		for (int i = 0; i < ArrayMap.MAX_ENTRY_COUNT; i++)
//		{
//			ArrayMapKey key = new ArrayMapKey("" + i);
//
//			OpResult result = map.put(new ArrayMapEntry(key, value, (byte)77));
//
//			assertNotEquals(result.state, State.OVERFLOW);
//		}
//
//		ArrayMapKey key = new ArrayMapKey("" + ArrayMap.MAX_ENTRY_COUNT);
//
//		assertEquals(map.put(new ArrayMapEntry(key, value, (byte)77)), State.OVERFLOW);
//	}
	@Test
	public void testIterator() throws UnsupportedEncodingException
	{
		ArrayMap map = new ArrayMap(1000_000, 1);

		HashMap<ArrayMapKey, Document> expected = new HashMap<>();

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
		ArrayMap map = new ArrayMap(1000_000, 1);

		HashMap<ArrayMapKey, Document> values = new HashMap<>();

		int n = map.getCapacity() - 2 - 4 - 2 - 2 - 4;

		ArrayMapKey key = new ArrayMapKey(doc(n / 2));
		Document value = doc(n - n / 2 + 1); // one byte exceeding maximum size
		byte flags = (byte)77;

		map.put(new ArrayMapEntry(key, value, flags));

		values.put(key, value);

		fail();
	}


	@Test
	public void testReplaceEntry() throws UnsupportedEncodingException
	{
		ArrayMapKey[] keys = new ArrayMapKey[1000];
		for (int i = 0; i < keys.length; i++)
		{
			keys[i] = new ArrayMapKey(t());
		}

		ArrayMap map = new ArrayMap(1000_000, 1);

		HashMap<ArrayMapKey, Document> values = new HashMap<>();

		byte type = (byte)77;

		for (int i = 0; i < 100_000; i++)
		{
			int j = c() % keys.length;
			Document value = doc();
			ArrayMapKey key = keys[j];

			if (map.put(new ArrayMapEntry(key, value, type)).state != OpState.OVERFLOW)
			{
				values.put(key, value);
			}
		}

		assertNull(map.integrityCheck());

		for (Entry<ArrayMapKey, Document> entry : values.entrySet())
		{
			OpResult entry1 = map.get(entry.getKey());
			assertEquals(entry1.state, OpState.MATCH);
			assertEquals(entry1.entry.getValue(), entry.getValue());
			assertEquals(entry1.entry.getType(), type);
		}
	}


	@Test
	public void testKeyOrder() throws IOException
	{
		Document value = doc();

		ArrayMap map = new ArrayMap(new byte[512], 1);
		map.put(new ArrayMapEntry(new ArrayMapKey("eeee"), value, (byte)77));
		map.put(new ArrayMapEntry(new ArrayMapKey("c"), value, (byte)77));
		map.put(new ArrayMapEntry(new ArrayMapKey("aaaaa"), value, (byte)77));
		map.put(new ArrayMapEntry(new ArrayMapKey("dd"), value, (byte)77));
		map.put(new ArrayMapEntry(new ArrayMapKey("bbb"), value, (byte)77));
		map.put(new ArrayMapEntry(new ArrayMapKey("ddd"), value, (byte)77));

		assertEquals(map.getKey(0).get(), "aaaaa");
		assertEquals(map.getKey(1).get(), "bbb");
		assertEquals(map.getKey(2).get(), "c");
		assertEquals(map.getKey(3).get(), "dd");
		assertEquals(map.getKey(4).get(), "ddd");
		assertEquals(map.getKey(5).get(), "eeee");
	}


	public static void fillArrayMap(ArrayMap aMap, HashMap<ArrayMapKey, Document> aValues)
	{
		for (;;)
		{
			ArrayMapKey key = new ArrayMapKey(t());
			Document value = doc();
			byte type = b();

			ArrayMapEntry entry = new ArrayMapEntry(key, value, type);

			if (entry.getMarshalledLength() > aMap.getCapacity() - ArrayMap.HEADER_SIZE - ArrayMap.ENTRY_HEADER_SIZE - ArrayMap.ENTRY_POINTER_SIZE)
			{
				continue;
			}

			if (aMap.put(entry).state == OpState.OVERFLOW)
			{
				break;
			}

			aValues.put(key, value);
		}
	}


	@Test
	public void testNearestIndex()
	{
		ArrayMap map = new ArrayMap(100, 1);
		map.put(new ArrayMapEntry(new ArrayMapKey("a"), Document.of("_id:a"), (byte)0));
		map.put(new ArrayMapEntry(new ArrayMapKey("c"), Document.of("_id:c"), (byte)0));
		map.put(new ArrayMapEntry(new ArrayMapKey("e"), Document.of("_id:e"), (byte)0));

		assertEquals(0, map.nearestIndex(new ArrayMapKey("a")));
		assertEquals(0, map.nearestIndex(new ArrayMapKey("b")));
		assertEquals(1, map.nearestIndex(new ArrayMapKey("c")));
		assertEquals(1, map.nearestIndex(new ArrayMapKey("d")));
		assertEquals(2, map.nearestIndex(new ArrayMapKey("e")));
		assertEquals(2, map.nearestIndex(new ArrayMapKey("f")));
	}


	@Test
	public void testNearest()
	{
		ArrayMap map = new ArrayMap(100, 1);
		map.put(new ArrayMapEntry(new ArrayMapKey("a"), Document.of("_id:a"), (byte)0));
		map.put(new ArrayMapEntry(new ArrayMapKey("c"), Document.of("_id:c"), (byte)0));
		map.put(new ArrayMapEntry(new ArrayMapKey("e"), Document.of("_id:e"), (byte)0));

		ArrayMapEntry a = new ArrayMapEntry(new ArrayMapKey("a"));
		map.loadNearestEntry(a);
		ArrayMapEntry b = new ArrayMapEntry(new ArrayMapKey("b"));
		map.loadNearestEntry(b);
		ArrayMapEntry c = new ArrayMapEntry(new ArrayMapKey("c"));
		map.loadNearestEntry(c);
		ArrayMapEntry d = new ArrayMapEntry(new ArrayMapKey("d"));
		map.loadNearestEntry(d);
		ArrayMapEntry e = new ArrayMapEntry(new ArrayMapKey("e"));
		map.loadNearestEntry(e);
		ArrayMapEntry f = new ArrayMapEntry(new ArrayMapKey("f"));
		map.loadNearestEntry(f);

		System.out.println(a);
		System.out.println(b);
		System.out.println(c);
		System.out.println(d);
		System.out.println(e);
		System.out.println(f);
	}


	@Test
	public void testNextEntry()
	{
		ArrayMap map = new ArrayMap(100, 1);
		map.put(new ArrayMapEntry(new ArrayMapKey("b"), Document.of("_id:a"), (byte)0));
		map.put(new ArrayMapEntry(new ArrayMapKey("d"), Document.of("_id:c"), (byte)0));
		map.put(new ArrayMapEntry(new ArrayMapKey("f"), Document.of("_id:e"), (byte)0));

		String[] s =
		{
			"b", "d", "f", "*"
		};

		assertEquals(s[map.findEntry(new ArrayMapKey("a"))], "b");
		assertEquals(s[map.findEntry(new ArrayMapKey("b"))], "b");
		assertEquals(s[map.findEntry(new ArrayMapKey("c"))], "d");
		assertEquals(s[map.findEntry(new ArrayMapKey("d"))], "d");
		assertEquals(s[map.findEntry(new ArrayMapKey("e"))], "f");
		assertEquals(s[map.findEntry(new ArrayMapKey("f"))], "f");
		assertEquals(s[map.findEntry(new ArrayMapKey("g"))], "*");
	}
}
