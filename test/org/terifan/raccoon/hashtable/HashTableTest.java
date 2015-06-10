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


public class HashTableTest
{
	public HashTableTest()
	{
//		Log.LEVEL = 10;
	}


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

//		Log.hexDump(root.encode(new byte[BlockPointer.SIZE], 0));

		try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice); HashTable hashTable = new HashTable(new BlockAccessor(managedBlockDevice), root, seed, nodeSize, leafSize, tx))
		{
			byte[] value = hashTable.get("key".getBytes());

			assertEquals("value", new String(value));
		}
	}


	@Test
	public void testRollback() throws Exception
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

			hashTable.put("key".getBytes(), "xxx".getBytes(), tx);
			assertEquals("xxx", new String(hashTable.get("key".getBytes())));
			hashTable.rollback();

			hashTable.remove("key".getBytes(), tx);
			assertEquals(null, hashTable.get("key".getBytes()));
			hashTable.rollback();

			assertEquals("value", new String(hashTable.get("key".getBytes())));
		}
	}


	@Test
	public void testSize() throws Exception
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

			managedBlockDevice.commit();
		}
	}


	@Test
	public void testIterator() throws Exception
	{
		HashMap<String,String> map = new HashMap<>();
		for (int i = 0; i < 1000; i++)
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
				assertEquals(new String(entry.getValue()), map.remove(new String(entry.getKey())));
			}
		}
	}
}
