package org.terifan.raccoon.hashtable;

import java.util.HashMap;
import java.util.Map;
import org.terifan.raccoon.io.BlockPointer;
import java.util.Random;
import org.terifan.raccoon.Entry;
import org.terifan.raccoon.io.BlockAccessor;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.io.ManagedBlockDevice;
import org.terifan.raccoon.io.MemoryBlockDevice;
import static org.terifan.raccoon.serialization.TestUtils.*;
import org.terifan.raccoon.util.Log;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;


public class HashTableTest
{
	@Test
	public void testSimpleCreateWriteOpenRead() throws Exception
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		BlockPointer root = null;
		long seed = new Random().nextLong();
		int nodeSize = 512;
		int leafSize = 1024;

		long tx = 0;

		try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice); HashTable hashTable = new HashTable(new BlockAccessor(managedBlockDevice), root, seed, nodeSize, leafSize, tx))
		{
			hashTable.put("key".getBytes(), "value".getBytes(), tx);
			hashTable.commit(tx);

			root = hashTable.getRootBlockPointer();

			managedBlockDevice.commit();
		}

		try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice); HashTable hashTable = new HashTable(new BlockAccessor(managedBlockDevice), root, seed, nodeSize, leafSize, tx))
		{
			byte[] value = hashTable.get("key".getBytes());

			assertEquals("value", new String(value));
		}
	}


	@Test(dataProvider="rollbackSizes")
	public void testRollback(int aSize) throws Exception
	{
		HashMap<String,String> map = new HashMap<>();
		for (int i = 0; i < aSize; i++)
		{
			map.put(t(), t());
		}

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		BlockPointer root = null;
		long seed = new Random().nextLong();
		int nodeSize = 512;
		int leafSize = 1024;
		long tx = 0;

		try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice); HashTable hashTable = new HashTable(new BlockAccessor(managedBlockDevice), root, seed, nodeSize, leafSize, tx))
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
			assertEquals(new String(hashTable.get(map.keySet().iterator().next().getBytes())), "err");
			hashTable.rollback();

			for (Map.Entry<String,String> entry : map.entrySet())
			{
				assertEquals(new String(hashTable.get(entry.getKey().getBytes())), entry.getValue());
			}

			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.remove(entry.getKey().getBytes(), tx);
			}
			assertEquals(hashTable.get(map.keySet().iterator().next().getBytes()), null);
			hashTable.rollback();

			for (Map.Entry<String,String> entry : map.entrySet())
			{
				assertEquals(new String(hashTable.get(entry.getKey().getBytes())), entry.getValue());
			}
		}
	}


	@Test(dataProvider = "sizeSizes")
	public void testSize(int aSize) throws Exception
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		BlockPointer root = null;
		long seed = new Random().nextLong();
		int nodeSize = 512;
		int leafSize = 1024;
		long tx = 0;

		try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice); HashTable hashTable = new HashTable(new BlockAccessor(managedBlockDevice), root, seed, nodeSize, leafSize, tx))
		{
			hashTable.put("a".getBytes(), "A".getBytes(), tx);

			assertEquals(hashTable.size(), 1);

			hashTable.put("b".getBytes(), "B".getBytes(), tx);

			hashTable.commit(tx);

			assertEquals(hashTable.size(), 2);

			managedBlockDevice.commit();
			root = hashTable.getRootBlockPointer();
		}

		try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice); HashTable hashTable = new HashTable(new BlockAccessor(managedBlockDevice), root, seed, nodeSize, leafSize, tx))
		{
			assertEquals(hashTable.size(), 2);

			hashTable.put("c".getBytes(), "C".getBytes(), tx);

			assertEquals(hashTable.size(), 3);

			hashTable.commit(tx);

			hashTable.put("b".getBytes(), "B".getBytes(), tx); // replace value

			assertEquals(hashTable.size(), 3);

			hashTable.put("d".getBytes(), "D".getBytes(), tx);

			hashTable.rollback();

			assertEquals(hashTable.size(), 3);

			for (int i = 0; i < aSize; i++)
			{
				hashTable.put(tb(), tb(), tx);
			}
			hashTable.commit(tx);

			assertEquals(hashTable.size(), 3 + aSize);

			managedBlockDevice.commit();
		}
	}


	@Test(dataProvider = "iteratorSizes")
	public void testIterator(int aSize) throws Exception
	{
		HashMap<String,String> map = new HashMap<>();
		for (int i = 0; i < aSize; i++)
		{
			map.put(t(), t());
		}

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		BlockPointer root = null;
		long seed = new Random().nextLong();
		int nodeSize = 512;
		int leafSize = 1024;
		long tx = 0;

		try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice); HashTable hashTable = new HashTable(new BlockAccessor(managedBlockDevice), root, seed, nodeSize, leafSize, tx))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(entry.getKey().getBytes(), entry.getValue().getBytes(), tx);
			}

			hashTable.commit(tx);
			managedBlockDevice.commit();
			root = hashTable.getRootBlockPointer();
		}

		try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice); HashTable hashTable = new HashTable(new BlockAccessor(managedBlockDevice), root, seed, nodeSize, leafSize, tx))
		{
			for (Entry entry : hashTable)
			{
				assertEquals(map.get(new String(entry.getKey())), new String(entry.getValue()));
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

		BlockPointer root = null;
		long seed = new Random().nextLong();
		int nodeSize = 512;
		int leafSize = 1024;
		long tx = 0;

		try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice); HashTable hashTable = new HashTable(new BlockAccessor(managedBlockDevice), root, seed, nodeSize, leafSize, tx))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(entry.getKey().getBytes(), entry.getValue().getBytes(), tx);
			}

			hashTable.commit(tx);
			managedBlockDevice.commit();
			root = hashTable.getRootBlockPointer();
		}

		try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice); HashTable hashTable = new HashTable(new BlockAccessor(managedBlockDevice), root, seed, nodeSize, leafSize, tx))
		{
			for (Entry entry : hashTable.list())
			{
				assertEquals(map.get(new String(entry.getKey())), new String(entry.getValue()));
			}
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


	@DataProvider(name="rollbackSizes")
	private Object[][] rollbackSizes()
	{
		return new Object[][]{{1},{10},{1000}};
	}
}
