package org.terifan.raccoon.hashtable;

import java.util.Random;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.io.ManagedBlockDevice;
import org.terifan.raccoon.io.MemoryBlockDevice;
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
	public void testSimpleCreateWriteOpenRead1() throws Exception
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		BlockPointer root = null;
		long seed = new Random().nextLong();
		int nodeSize = 512;
		int leafSize = 1024;

		long tx = -1;

		try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice); HashTable hashTable = new HashTable(managedBlockDevice, root, seed, nodeSize, leafSize, tx))
		{
			hashTable.put("key".getBytes(), "value".getBytes(), tx);
			hashTable.commit(tx);

			root = hashTable.getRootBlockPointer();

			managedBlockDevice.commit();
		}

//		Log.hexDump(root.encode(new byte[BlockPointer.SIZE], 0));

		try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice); HashTable hashTable = new HashTable(managedBlockDevice, root, seed, nodeSize, leafSize, tx))
		{
			byte[] value = hashTable.get("key".getBytes());

			assertEquals("value", new String(value));
		}
	}


	@Test
	public void testRollback1() throws Exception
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		BlockPointer root = null;
		long seed = new Random().nextLong();
		int nodeSize = 512;
		int leafSize = 1024;

		long tx = -1;

		try (IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice); HashTable hashTable = new HashTable(managedBlockDevice, root, seed, nodeSize, leafSize, tx))
		{
			hashTable.put("key".getBytes(), "value".getBytes(), tx);
			hashTable.commit(tx);

			hashTable.put("key".getBytes(), "xxx".getBytes(), tx);
			hashTable.rollback();

			byte[] value = hashTable.get("key".getBytes());

			assertEquals("value", new String(value));

			managedBlockDevice.commit();
		}
	}
}
