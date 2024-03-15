package org.terifan.raccoon.btree;

import org.testng.annotations.Test;
import static org.testng.Assert.*;
import static org.terifan.raccoon._Tools.doc;
import org.terifan.raccoon.btree.ArrayMapEntry.Type;
import org.terifan.raccoon.document.Document;


public class ArrayMapNGTest
{
	public ArrayMapNGTest()
	{
		ArrayMap.class.getClassLoader().setClassAssertionStatus(ArrayMap.class.getName(), false);
	}


	@Test
	public void testPutGetReplaceRemove()
	{
		ArrayMap map = new ArrayMap(10, 100);

		Object org = "hello world";
		ArrayMapEntry out = new ArrayMapEntry().setKeyInstance("apple").setValueInstance(org);

		map.put(out);

		assertEquals(out.getState(), OpState.OVERFLOW);

		map.insert(out);

		assertEquals(map.size(), 1);
		assertEquals(out.getState(), OpState.INSERT);
		assertEquals(out.getValueType(), Type.STRING);

		ArrayMapEntry existing = new ArrayMapEntry().setKey(out.getKey(), out.getKeyType());
		map.get(existing);

		assertEquals(existing.getState(), OpState.MATCH);
		assertEquals(existing.getValue(), out.getValue());
		assertEquals(existing.getValueType(), out.getValueType());

		Object val = "***********************".getBytes();
		out.setValueInstance(val);

		map.put(out);

		assertEquals(out.getState(), OpState.UPDATE);
		assertEquals(out.getValueType(), Type.STRING);
		assertEquals(out.getValueInstance(), org);

		map.get(existing);

		assertEquals(existing.getState(), OpState.MATCH);
		assertEquals(existing.getValueType(), Type.BYTEARRAY);
		assertEquals(existing.getValueInstance(), val);

		map.remove(existing);

		assertEquals(existing.getState(), OpState.REMOVED);
		assertEquals(existing.getValueType(), Type.BYTEARRAY);
		assertEquals(existing.getValueInstance(), val);

		map.remove(existing);

		assertEquals(existing.getState(), OpState.NO_MATCH);
	}


//	@Test
//	public void testPutRemove()
//	{
//		ArrayMap map = new ArrayMap(1000_000, 1);
//
//		String key = t();
//		Document value = doc();
//		byte flags = (byte)77;
//
//		_ArrayMapEntry entry = new _ArrayMapEntry(new _ArrayMapKey(key), value, flags);
//
//		Entry op = map.put(entry);
//
//		assertNotEquals(op.state, OpState.OVERFLOW);
//
//		op = map.remove(entry.getKey());
//
//		assertEquals(op.state, OpState.DELETE);
//
////		assertEquals(entry, op.entry);
//	}
//
//
//	@Test
//	public void testRemoveNonExisting()
//	{
//		ArrayMap map = new ArrayMap(1000_000, 1);
//
//		String key = t();
//
//		_ArrayMapEntry entry = new _ArrayMapEntry(new _ArrayMapKey(key));
//
//		Entry old = map.remove(entry.getKey());
//
//		assertEquals(old.state, OpState.NO_MATCH);
//		assertNull(old.entry);
//	}
//
//
//	@Test
//	public void testFillBuffer() throws UnsupportedEncodingException
//	{
//		ArrayMap map = new ArrayMap(1000_000, 1);
//
//		HashMap<_ArrayMapKey, Document> expected = new HashMap<>();
//
//		fillArrayMap(map, expected);
//
//		for (Entry<_ArrayMapKey, Document> expectedEntry : expected.entrySet())
//		{
//			Entry entry = map.get(expectedEntry.getKey());
//
//			assertEquals(entry.state, OpState.MATCH);
//			assertEquals(entry.entry.getValue(), expectedEntry.getValue());
//		}
//	}
//
//
//	@Test
//	public void testIterator() throws UnsupportedEncodingException
//	{
//		ArrayMap map = new ArrayMap(1000_000, 1);
//
//		HashMap<_ArrayMapKey, Document> expected = new HashMap<>();
//
//		fillArrayMap(map, expected);
//
//		HashSet<_ArrayMapKey> found = new HashSet<>();
//
//		for (_ArrayMapEntry entry : map)
//		{
//			_ArrayMapKey k = entry.getKey();
//			assertEquals(entry.getValue(), expected.get(k));
//			found.add(k);
//		}
//
//		assertEquals(found, expected.keySet());
//	}
//
//
//	@Test(expectedExceptions = IllegalArgumentException.class)
//	public void testExceedBufferSize()
//	{
//		ArrayMap map = new ArrayMap(1000_000, 1);
//
//		HashMap<_ArrayMapKey, Document> values = new HashMap<>();
//
//		int n = map.getCapacity() - 2 - 4 - 2 - 2 - 4;
//
//		_ArrayMapKey key = new _ArrayMapKey(doc(n / 2));
//		Document value = doc(n - n / 2 + 1); // one byte exceeding maximum size
//		byte flags = (byte)77;
//
//		map.put(new _ArrayMapEntry(key, value, flags));
//
//		values.put(key, value);
//
//		fail();
//	}
//
//
//	@Test
//	public void testReplaceEntry() throws UnsupportedEncodingException
//	{
//		_ArrayMapKey[] keys = new _ArrayMapKey[1000];
//		for (int i = 0; i < keys.length; i++)
//		{
//			keys[i] = new _ArrayMapKey(t());
//		}
//
//		ArrayMap map = new ArrayMap(1000_000, 1);
//
//		HashMap<_ArrayMapKey, Document> values = new HashMap<>();
//
//		byte type = (byte)77;
//
//		for (int i = 0; i < 100_000; i++)
//		{
//			int j = c() % keys.length;
//			Document value = doc();
//			_ArrayMapKey key = keys[j];
//
//			if (map.put(new _ArrayMapEntry(key, value, type)).state != OpState.OVERFLOW)
//			{
//				values.put(key, value);
//			}
//		}
//
//		assertNull(map.integrityCheck());
//
//		for (Entry<_ArrayMapKey, Document> entry : values.entrySet())
//		{
//			Entry entry1 = map.get(entry.getKey());
//			assertEquals(entry1.state, OpState.MATCH);
//			assertEquals(entry1.entry.getValue(), entry.getValue());
//			assertEquals(entry1.entry.getType(), type);
//		}
//	}
//
//
//	@Test
//	public void testKeyOrder() throws IOException
//	{
//		Document value = doc();
//
//		ArrayMap map = new ArrayMap(new byte[512], 1);
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("eeee"), value, (byte)77));
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("c"), value, (byte)77));
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("aaaaa"), value, (byte)77));
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("dd"), value, (byte)77));
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("bbb"), value, (byte)77));
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("ddd"), value, (byte)77));
//
//		assertEquals(map.getKey(0).get(), "aaaaa");
//		assertEquals(map.getKey(1).get(), "bbb");
//		assertEquals(map.getKey(2).get(), "c");
//		assertEquals(map.getKey(3).get(), "dd");
//		assertEquals(map.getKey(4).get(), "ddd");
//		assertEquals(map.getKey(5).get(), "eeee");
//	}
//
//
//	public static void fillArrayMap(ArrayMap aMap, HashMap<_ArrayMapKey, Document> aValues)
//	{
//		for (;;)
//		{
//			_ArrayMapKey key = new _ArrayMapKey(t());
//			Document value = doc();
//			byte type = b();
//
//			_ArrayMapEntry entry = new _ArrayMapEntry(key, value, type);
//
//			if (entry.getMarshalledLength() > aMap.getCapacity() - ArrayMap.HEADER_SIZE - ArrayMap.ENTRY_HEADER_SIZE - ArrayMap.ENTRY_POINTER_SIZE)
//			{
//				continue;
//			}
//
//			if (aMap.put(entry).state == OpState.OVERFLOW)
//			{
//				break;
//			}
//
//			aValues.put(key, value);
//		}
//	}
//
//
//	@Test
//	public void testNearestIndex()
//	{
//		ArrayMap map = new ArrayMap(100, 1);
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("a"), Document.of("_id:a"), (byte)0));
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("c"), Document.of("_id:c"), (byte)0));
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("e"), Document.of("_id:e"), (byte)0));
//
//		assertEquals(0, map.nearestIndex(new _ArrayMapKey("a")));
//		assertEquals(0, map.nearestIndex(new _ArrayMapKey("b")));
//		assertEquals(1, map.nearestIndex(new _ArrayMapKey("c")));
//		assertEquals(1, map.nearestIndex(new _ArrayMapKey("d")));
//		assertEquals(2, map.nearestIndex(new _ArrayMapKey("e")));
//		assertEquals(2, map.nearestIndex(new _ArrayMapKey("f")));
//	}
//
//
//	@Test
//	public void testNearest()
//	{
//		ArrayMap map = new ArrayMap(100, 1);
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("a"), Document.of("_id:a"), (byte)0));
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("c"), Document.of("_id:c"), (byte)0));
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("e"), Document.of("_id:e"), (byte)0));
//
//		_ArrayMapEntry a = new _ArrayMapEntry(new _ArrayMapKey("a"));
//		map.loadNearestEntry(a);
//		_ArrayMapEntry b = new _ArrayMapEntry(new _ArrayMapKey("b"));
//		map.loadNearestEntry(b);
//		_ArrayMapEntry c = new _ArrayMapEntry(new _ArrayMapKey("c"));
//		map.loadNearestEntry(c);
//		_ArrayMapEntry d = new _ArrayMapEntry(new _ArrayMapKey("d"));
//		map.loadNearestEntry(d);
//		_ArrayMapEntry e = new _ArrayMapEntry(new _ArrayMapKey("e"));
//		map.loadNearestEntry(e);
//		_ArrayMapEntry f = new _ArrayMapEntry(new _ArrayMapKey("f"));
//		map.loadNearestEntry(f);
//
//		System.out.println(a);
//		System.out.println(b);
//		System.out.println(c);
//		System.out.println(d);
//		System.out.println(e);
//		System.out.println(f);
//	}
//
//
//	@Test
//	public void testNextEntry()
//	{
//		ArrayMap map = new ArrayMap(100, 1);
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("b"), Document.of("_id:a"), (byte)0));
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("d"), Document.of("_id:c"), (byte)0));
//		map.put(new _ArrayMapEntry(new _ArrayMapKey("f"), Document.of("_id:e"), (byte)0));
//
//		String[] s =
//		{
//			"b", "d", "f", "*"
//		};
//
//		assertEquals(s[map.findEntry(new _ArrayMapKey("a"))], "b");
//		assertEquals(s[map.findEntry(new _ArrayMapKey("b"))], "b");
//		assertEquals(s[map.findEntry(new _ArrayMapKey("c"))], "d");
//		assertEquals(s[map.findEntry(new _ArrayMapKey("d"))], "d");
//		assertEquals(s[map.findEntry(new _ArrayMapKey("e"))], "f");
//		assertEquals(s[map.findEntry(new _ArrayMapKey("f"))], "f");
//		assertEquals(s[map.findEntry(new _ArrayMapKey("g"))], "*");
//	}
}
