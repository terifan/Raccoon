package org.terifan.raccoon;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.io.ManagedBlockDevice;
import org.terifan.raccoon.io.MemoryBlockDevice;
import static tests.__TestUtils.*;
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
		TransactionCounter tx = new TransactionCounter(0);

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (int i = 0; i < aSize; i++)
			{
				byte[] key = tb();
				byte[] value = tb();
				hashTable.put(new LeafEntry(key, value, (byte)0));
				map.put(key, value);
			}

			hashTable.commit();
			root = hashTable.getTableHeader();
		}

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (byte[] key : map.keySet())
			{
				assertEquals(get(hashTable, key), map.get(key));
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
		TransactionCounter tx = new TransactionCounter(0);

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(new LeafEntry(entry.getKey().getBytes(), entry.getValue().getBytes(), (byte)0));
			}
			hashTable.commit();

			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(new LeafEntry(entry.getKey().getBytes(), "err".getBytes(), (byte)0));
			}
			assertEquals(Log.toString(get(hashTable, map.keySet().iterator().next().getBytes())), "err");
			hashTable.rollback();

			for (Map.Entry<String,String> entry : map.entrySet())
			{
				assertEquals(Log.toString(get(hashTable, entry.getKey().getBytes())), entry.getValue());
			}

			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.remove(new LeafEntry(entry.getKey().getBytes()));
			}
			assertEquals(get(hashTable, map.keySet().iterator().next().getBytes()), null);
			hashTable.rollback();

			for (Map.Entry<String,String> entry : map.entrySet())
			{
				assertEquals(Log.toString(get(hashTable, entry.getKey().getBytes())), entry.getValue());
			}
		}
	}


	@Test(dataProvider = "sizeSizes")
	public void testSize(int aSize) throws Exception
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		TransactionCounter tx = new TransactionCounter(0);

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
			hashTable.put(new LeafEntry(k0, v0, (byte)0));

			assertEquals(hashTable.size(), 1);

			hashTable.put(new LeafEntry(k1, v1, (byte)0));

			hashTable.commit();

			assertEquals(hashTable.size(), 2);

			root = hashTable.getTableHeader();
		}

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			assertEquals(hashTable.size(), 2);

			hashTable.put(new LeafEntry(k2, v2, (byte)0));

			assertEquals(hashTable.size(), 3);

			hashTable.commit();

			hashTable.put(new LeafEntry(k1, v1, (byte)0)); // replace value

			assertEquals(hashTable.size(), 3);

			hashTable.put(new LeafEntry(k3, v3, (byte)0));

			hashTable.rollback();

			assertEquals(hashTable.size(), 3);

			for (int i = 0; i < aSize; i++)
			{
				hashTable.put(new LeafEntry(tb(), tb(), (byte)0));
			}
			hashTable.commit();

			assertEquals(hashTable.size(), 3 + aSize);
		}
	}


	@Test(dataProvider = "iteratorSizes")
	public void testIterator(int aSize) throws Exception
	{
		HashMap<String,String> map = new HashMap<>();

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		TransactionCounter tx = new TransactionCounter(0);

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (int i = 0; i < aSize; i++)
			{
				String key = t();
				String value = t();

				map.put(key, value);

				hashTable.put(new LeafEntry(key.getBytes(), value.getBytes(), (byte)0));
			}

			hashTable.commit();
			root = hashTable.getTableHeader();
		}

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (LeafEntry entry : hashTable)
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
		TransactionCounter tx = new TransactionCounter(0);

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(new LeafEntry(entry.getKey().getBytes(), entry.getValue().getBytes(), (byte)0));
			}

			hashTable.commit();
			root = hashTable.getTableHeader();
		}

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (LeafEntry entry : hashTable.list())
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
		TransactionCounter tx = new TransactionCounter(0);

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(new LeafEntry(entry.getKey().getBytes(), entry.getValue().getBytes(), (byte)0));
			}

			hashTable.commit();
			root = hashTable.getTableHeader();
		}

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			hashTable.clear();
			hashTable.commit();

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
		TransactionCounter tx = new TransactionCounter(0);

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(new LeafEntry(entry.getKey().getBytes(), entry.getValue().getBytes(), (byte)0));
			}

			hashTable.commit();
			root = hashTable.getTableHeader();
		}

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				LeafEntry leafEntry = new LeafEntry(entry.getKey().getBytes(), entry.getValue().getBytes(), (byte)0);
				assertTrue(hashTable.remove(leafEntry));
				assertEquals(leafEntry.getValue(), entry.getValue().getBytes());
			}
			hashTable.commit();

			assertEquals(hashTable.size(), 0);
		}
	}


	@Test
	public void testPut() throws Exception
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
		TransactionCounter tx = new TransactionCounter(0);
		byte[] root = null;

		try (HashTable hashTable = newHashTable(root, tx, blockDevice))
		{
			byte[] key = new byte[hashTable.getEntryMaximumLength() - 1];
			byte[] value = {85};

			hashTable.put(new LeafEntry(key, value, (byte)0));

			assertEquals(get(hashTable, key), value);
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


	private HashTable newHashTable(byte[] aRoot, TransactionCounter aTransactionId, MemoryBlockDevice aBlockDevice) throws IOException
	{
		return new HashTable(new ManagedBlockDevice(aBlockDevice), aRoot, aTransactionId, true, null);
	}


	private byte[] get(HashTable aHashTable, byte[] aKey)
	{
		LeafEntry leafEntry = new LeafEntry(aKey);
		if (aHashTable.get(leafEntry))
		{
			return leafEntry.getValue();
		}
		return null;
	}
}
