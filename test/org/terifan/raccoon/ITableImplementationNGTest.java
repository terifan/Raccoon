package org.terifan.raccoon;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import static resources.__TestUtils.*;
import org.terifan.raccoon.util.Log;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;


public class ITableImplementationNGTest
{
	@Test(dataProvider = "itemSizes")
	public void testSimpleCreateWriteOpenRead(Class<ITableImplementation> aTable, int aSize) throws Exception
	{
		HashMap<byte[],byte[]> map = new HashMap<>();

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		TransactionGroup tx = new TransactionGroup(0);

		try (ITableImplementation hashTable = newHashTable(aTable, root, tx, blockDevice))
		{
			for (int i = 0; i < aSize; i++)
			{
				byte[] key = tb();
				byte[] value = tb();
				hashTable.put(new ArrayMapEntry(key, value, (byte)0));
				map.put(key, value);
			}

			root = hashTable.commit(null);
		}

		try (ITableImplementation hashTable = newHashTable(aTable, root, tx, blockDevice))
		{
			for (byte[] key : map.keySet())
			{
				assertEquals(get(hashTable, key), map.get(key));
			}
		}
	}


	@Test(dataProvider="itemSizes")
	public void testRollback(Class<ITableImplementation> aTable, int aSize) throws Exception
	{
		HashMap<String,String> map = new HashMap<>();
		for (int i = 0; i < aSize; i++)
		{
			map.put(t(), t());
		}

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		TransactionGroup tx = new TransactionGroup(0);

		try (ITableImplementation hashTable = newHashTable(aTable, root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(new ArrayMapEntry(entry.getKey().getBytes(), entry.getValue().getBytes(), (byte)0));
			}
			hashTable.commit(null);

			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(new ArrayMapEntry(entry.getKey().getBytes(), "err".getBytes(), (byte)0));
			}
			assertEquals(Log.toString(get(hashTable, map.keySet().iterator().next().getBytes())), "err");
			hashTable.rollback();

			for (Map.Entry<String,String> entry : map.entrySet())
			{
				assertEquals(Log.toString(get(hashTable, entry.getKey().getBytes())), entry.getValue());
			}

			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.remove(new ArrayMapEntry(entry.getKey().getBytes()));
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
	public void testSize(Class<ITableImplementation> aTable, int aSize) throws Exception
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		TransactionGroup tx = new TransactionGroup(0);

		byte[] k0 = tb();
		byte[] k1 = tb();
		byte[] k2 = tb();
		byte[] k3 = tb();
		byte[] v0 = tb();
		byte[] v1 = tb();
		byte[] v2 = tb();
		byte[] v3 = tb();

		try (ITableImplementation hashTable = newHashTable(aTable, root, tx, blockDevice))
		{
			hashTable.put(new ArrayMapEntry(k0, v0, (byte)0));

			assertEquals(hashTable.size(), 1);

			hashTable.put(new ArrayMapEntry(k1, v1, (byte)0));

			root = hashTable.commit(null);

			assertEquals(hashTable.size(), 2);
		}

		try (ITableImplementation hashTable = newHashTable(aTable, root, tx, blockDevice))
		{
			assertEquals(hashTable.size(), 2);

			hashTable.put(new ArrayMapEntry(k2, v2, (byte)0));

			assertEquals(hashTable.size(), 3);

			hashTable.commit(null);

			hashTable.put(new ArrayMapEntry(k1, v1, (byte)0)); // replace value

			assertEquals(hashTable.size(), 3);

			hashTable.put(new ArrayMapEntry(k3, v3, (byte)0));

			hashTable.rollback();

			assertEquals(hashTable.size(), 3);

			for (int i = 0; i < aSize; i++)
			{
				hashTable.put(new ArrayMapEntry(tb(), tb(), (byte)0));
			}
			hashTable.commit(null);

			assertEquals(hashTable.size(), 3 + aSize);
		}
	}


	@Test(dataProvider = "iteratorSizes")
	public void testIterator(Class<ITableImplementation> aTable, int aSize) throws Exception
	{
		HashMap<String,String> map = new HashMap<>();

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		TransactionGroup tx = new TransactionGroup(0);

		try (ITableImplementation hashTable = newHashTable(aTable, root, tx, blockDevice))
		{
			for (int i = 0; i < aSize; i++)
			{
				String key = t();
				String value = t();

				map.put(key, value);

				hashTable.put(new ArrayMapEntry(key.getBytes(), value.getBytes(), (byte)0));
			}

			root = hashTable.commit(null);
		}

		try (ITableImplementation hashTable = newHashTable(aTable, root, tx, blockDevice))
		{
			for (ArrayMapEntry entry : hashTable)
			{
				String key = Log.toString(entry.getKey());
				String value = Log.toString(entry.getValue());

				assertTrue(map.containsKey(key));
				assertEquals(map.get(key), value);
			}
		}
	}


	@Test(dataProvider = "iteratorSizes")
	public void testList(Class<ITableImplementation> aTable, int aSize) throws Exception
	{
		HashMap<String,String> map = new HashMap<>();
		for (int i = 0; i < aSize; i++)
		{
			map.put(t(), t());
		}

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		TransactionGroup tx = new TransactionGroup(0);

		try (ITableImplementation hashTable = newHashTable(aTable, root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(new ArrayMapEntry(entry.getKey().getBytes(), entry.getValue().getBytes(), (byte)0));
			}

			root = hashTable.commit(null);
		}

		try (ITableImplementation hashTable = newHashTable(aTable, root, tx, blockDevice))
		{
			for (ArrayMapEntry entry : hashTable.list())
			{
				String key = Log.toString(entry.getKey());
				String value = Log.toString(entry.getValue());

				assertTrue(map.containsKey(key));
				assertEquals(map.get(key), value);
			}
		}
	}


	@Test(dataProvider = "iteratorSizes")
	public void testClear(Class<ITableImplementation> aTable, int aSize) throws Exception
	{
		HashMap<String,String> map = new HashMap<>();
		for (int i = 0; i < aSize; i++)
		{
			map.put(t(), t());
		}

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		TransactionGroup tx = new TransactionGroup(0);

		try (ITableImplementation hashTable = newHashTable(aTable, root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(new ArrayMapEntry(entry.getKey().getBytes(), entry.getValue().getBytes(), (byte)0));
			}

			root = hashTable.commit(null);
		}

		try (ITableImplementation hashTable = newHashTable(aTable, root, tx, blockDevice))
		{
			hashTable.removeAll(c->{});
			hashTable.commit(null);

			assertEquals(hashTable.size(), 0);
		}
	}


	@Test(dataProvider = "iteratorSizes")
	public void testRemove(Class<ITableImplementation> aTable, int aSize) throws Exception
	{
		HashMap<String,String> map = new HashMap<>();
		for (int i = 0; i < aSize; i++)
		{
			map.put(t(), t());
		}

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		byte[] root = null;
		TransactionGroup tx = new TransactionGroup(0);

		try (ITableImplementation hashTable = newHashTable(aTable, root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				hashTable.put(new ArrayMapEntry(entry.getKey().getBytes(), entry.getValue().getBytes(), (byte)0));
			}

			root = hashTable.commit(null);
		}

		try (ITableImplementation hashTable = newHashTable(aTable, root, tx, blockDevice))
		{
			for (Map.Entry<String,String> entry : map.entrySet())
			{
				ArrayMapEntry leafEntry = new ArrayMapEntry(entry.getKey().getBytes(), entry.getValue().getBytes(), (byte)0);
				assertTrue(hashTable.remove(leafEntry) != null);
				assertEquals(leafEntry.getValue(), entry.getValue().getBytes());
			}
			hashTable.commit(null);

			assertEquals(hashTable.size(), 0);
		}
	}


	@Test(dataProvider = "tableTypes")
	public void testPut(Class<ITableImplementation> aTable) throws Exception
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
		TransactionGroup tx = new TransactionGroup(0);
		byte[] root = null;

		try (ITableImplementation hashTable = newHashTable(aTable, root, tx, blockDevice))
		{
			byte[] key = new byte[hashTable.getEntrySizeLimit() - 1];
			byte[] value = {85};

			hashTable.put(new ArrayMapEntry(key, value, (byte)0));

			assertEquals(get(hashTable, key), value);
		}
	}


	@DataProvider(name="iteratorSizes")
	private Object[][] iteratorSizes()
	{
		return new Object[][]{{ExtendibleHashTable.class,0},{ExtendibleHashTable.class,10},{ExtendibleHashTable.class,1000}};
	}


	@DataProvider(name="sizeSizes")
	private Object[][] sizeSizes()
	{
		return new Object[][]{{ExtendibleHashTable.class,0},{ExtendibleHashTable.class,1000}};
	}


	@DataProvider(name="itemSizes")
	private Object[][] itemSizes()
	{
		return new Object[][]{{ExtendibleHashTable.class,1},{ExtendibleHashTable.class,10},{ExtendibleHashTable.class,1000}};
	}


	@DataProvider(name="tableTypes")
	private Object[][] tableTypes()
	{
		return new Object[][]{{ExtendibleHashTable.class}};
	}


	private ITableImplementation newHashTable(Class<ITableImplementation> aImplementation, byte[] aRoot, TransactionGroup aTransactionId, MemoryBlockDevice aBlockDevice) throws IOException
	{
		try
		{
			ITableImplementation table = aImplementation.newInstance();
			if (aRoot == null)
			{
				table.create(new ManagedBlockDevice(aBlockDevice), aTransactionId, true, CompressionParam.BEST_SPEED, TableParam.DEFAULT, "noname", new Cost(), new PerformanceTool(null));
			}
			else
			{
				table.open(new ManagedBlockDevice(aBlockDevice), aTransactionId, true, CompressionParam.BEST_SPEED, TableParam.DEFAULT, "noname", new Cost(), new PerformanceTool(null), aRoot);
			}
			return table;
		}
		catch (Exception e)
		{
			throw new IllegalStateException(e);
		}
	}


	private byte[] get(ITableImplementation aHashTable, byte[] aKey)
	{
		ArrayMapEntry leafEntry = new ArrayMapEntry(aKey);
		if (aHashTable.get(leafEntry))
		{
			return leafEntry.getValue();
		}
		return null;
	}
}
