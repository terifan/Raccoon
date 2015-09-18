package org.terifan.raccoon.hashtable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.terifan.raccoon.Entry;
import org.terifan.raccoon.io.ManagedBlockDevice;
import org.terifan.raccoon.io.MemoryBlockDevice;
import static tests.__TestUtils.*;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;


public class HashTableNGTest
{
	@Test(dataProvider = "itemSizes")
	public void testSimpleCreateWriteOpenRead(int aSize) throws Exception
	{
		HashMap<byte[],byte[]> map = new HashMap<>();

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		long tx = 0;

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (int i = 0; i < aSize; i++)
			{
				byte[] key = tb();
				byte[] value = tb();
				hashTable.put(key, value, tx);
				map.put(key, value);
			}

			hashTable.commit(tx);
			root = hashTable.getTableHeader();
		}

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (byte[] key : map.keySet())
			{
				assertTrue(hashTable.containsKey(key));

				assertEquals(hashTable.get(key), map.get(key));
			}
		}
	}


	@Test(dataProvider="itemSizes")
	public void testRollback(int aSize) throws Exception
	{
		HashMap<String,String> map = new HashMap<>();
		for (int i = 0; i < aSize; i++)
		{
			map.put(t(), t());
		}

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		long tx = 0;

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(entry.getKey().getBytes(), entry.getValue().getBytes(), tx);
			}
			hashTable.commit(tx);

			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(entry.getKey().getBytes(), "err".getBytes(), tx);
			}
			assertEquals(Log.toString(hashTable.get(map.keySet().iterator().next().getBytes())), "err");
			hashTable.rollback();

			for (Map.Entry<String,String> entry : map.entrySet())
			{
				assertEquals(Log.toString(hashTable.get(entry.getKey().getBytes())), entry.getValue());
			}

			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.remove(entry.getKey().getBytes(), tx);
			}
			assertEquals(hashTable.get(map.keySet().iterator().next().getBytes()), null);
			hashTable.rollback();

			for (Map.Entry<String,String> entry : map.entrySet())
			{
				assertEquals(Log.toString(hashTable.get(entry.getKey().getBytes())), entry.getValue());
			}
		}
	}


	@Test(dataProvider = "sizeSizes")
	public void testSize(int aSize) throws Exception
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		long tx = 0;

		byte[] k0 = tb();
		byte[] k1 = tb();
		byte[] k2 = tb();
		byte[] k3 = tb();
		byte[] v0 = tb();
		byte[] v1 = tb();
		byte[] v2 = tb();
		byte[] v3 = tb();

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			hashTable.put(k0, v0, tx);

			assertEquals(hashTable.size(), 1);

			hashTable.put(k1, v1, tx);

			hashTable.commit(tx);

			assertEquals(hashTable.size(), 2);

			root = hashTable.getTableHeader();
		}

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			assertEquals(hashTable.size(), 2);

			hashTable.put(k2, v2, tx);

			assertEquals(hashTable.size(), 3);

			hashTable.commit(tx);

			hashTable.put(k1, v1, tx); // replace value

			assertEquals(hashTable.size(), 3);

			hashTable.put(k3, v3, tx);

			hashTable.rollback();

			assertEquals(hashTable.size(), 3);

			for (int i = 0; i < aSize; i++)
			{
				hashTable.put(tb(), tb(), tx);
			}
			hashTable.commit(tx);

			assertEquals(hashTable.size(), 3 + aSize);
		}
	}


	@Test(dataProvider = "iteratorSizes")
	public void testIterator(int aSize) throws Exception
	{
		HashMap<String,String> map = new HashMap<>();

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		long tx = 0;

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (int i = 0; i < aSize; i++)
			{
				String key = t();
				String value = t();

				map.put(key, value);

				hashTable.put(key.getBytes(), value.getBytes(), tx);
			}

			hashTable.commit(tx);
			root = hashTable.getTableHeader();
		}

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (Entry entry : hashTable)
			{
				String key = Log.toString(entry.getKey());
				String value = Log.toString(entry.getValue());

				assertTrue(map.containsKey(key));
				assertEquals(map.get(key), value);
			}
		}
	}


	@Test(dataProvider = "iteratorSizes")
	public void testList(int aSize) throws Exception
	{
		HashMap<String,String> map = new HashMap<>();
		for (int i = 0; i < aSize; i++)
		{
			map.put(t(), t());
		}

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		long tx = 0;

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(entry.getKey().getBytes(), entry.getValue().getBytes(), tx);
			}

			hashTable.commit(tx);
			root = hashTable.getTableHeader();
		}

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (Entry entry : hashTable.list())
			{
				String key = Log.toString(entry.getKey());
				String value = Log.toString(entry.getValue());

				assertTrue(map.containsKey(key));
				assertEquals(map.get(key), value);
			}
		}
	}


	@Test(dataProvider = "iteratorSizes")
	public void testClear(int aSize) throws Exception
	{
		HashMap<String,String> map = new HashMap<>();
		for (int i = 0; i < aSize; i++)
		{
			map.put(t(), t());
		}

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		long tx = 0;

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(entry.getKey().getBytes(), entry.getValue().getBytes(), tx);
			}

			hashTable.commit(tx);
			root = hashTable.getTableHeader();
		}

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			hashTable.clear(tx);
			hashTable.commit(tx);

			assertEquals(hashTable.size(), 0);
		}
	}


	@Test(dataProvider = "iteratorSizes")
	public void testRemove(int aSize) throws Exception
	{
		HashMap<String,String> map = new HashMap<>();
		for (int i = 0; i < aSize; i++)
		{
			map.put(t(), t());
		}

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		long tx = 0;

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(entry.getKey().getBytes(), entry.getValue().getBytes(), tx);
			}

			hashTable.commit(tx);
			root = hashTable.getTableHeader();
		}

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				assertEquals(hashTable.remove(entry.getKey().getBytes(), tx), entry.getValue().getBytes());
			}
			hashTable.commit(tx);

			assertEquals(hashTable.size(), 0);
		}
	}


	@DataProvider(name="iteratorSizes")
	private Object[][] iteratorSizes()
	{
		return new Object[][]{{0},{10},{1000}};
	}


	@DataProvider(name="sizeSizes")
	private Object[][] sizeSizes()
	{
		return new Object[][]{{0},{1000}};
	}


	@DataProvider(name="itemSizes")
	private Object[][] itemSizes()
	{
		return new Object[][]{{1},{10},{1000}};
	}


	private HashTable newHashTable(byte[] aRoot, long aTx, MemoryBlockDevice aBlockDevice) throws IOException
	{
		return new HashTable(new ManagedBlockDevice(aBlockDevice), aRoot == null ? null : new ByteArrayBuffer(aRoot), aTx, true);
	}
}