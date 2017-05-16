package org.terifan.raccoon.btree;

import java.io.IOException;
import org.terifan.raccoon.core.ArrayMap;
import org.terifan.raccoon.core.RecordEntry;
import org.testng.annotations.Test;
import static resources.__TestUtils.*;


public class BTreeNGTest
{
	@Test
	public void testSomeMethod() throws IOException
	{
//		Log.mLevel = LogLevel.DEBUG;

//		MemoryBlockDevice memoryBlockDevice = new MemoryBlockDevice(512);
//
//		IManagedBlockDevice blockDevice = new ManagedBlockDevice(memoryBlockDevice, null, 0);
//
//		BTree table = new BTree(blockDevice, null, new TransactionCounter(0), true, CompressionParam.NO_COMPRESSION, TableParam.DEFAULT);
//
//		for (int i = 0; i < 100; i++)
//		{
//			byte[] key = String.format("%04d", i).getBytes();
//
//			table.put(new RecordEntry(key, tb(), (byte)0));
//		}
//
//		table.commit();
//
//		memoryBlockDevice.dump();
	}


	@Test
	public void testSplitLeaf2() throws IOException
	{
		int S = 150;

		for (int i = 0; i < 10; i++)
		{
			ArrayMap map = new ArrayMap(S);

			for (int k = 0; k < 50; k++)
			{
				map.put(new RecordEntry(s('a' + k, 1 + rnd.nextInt(30)).getBytes(), "123".getBytes(), (byte)0));
			}

			String org = map.toString();

			RecordEntry re = new RecordEntry(s('c', 5).getBytes(), "123".getBytes(), (byte)0);

			ArrayMap[] maps = BTree.splitLeafImpl(map, re);

			System.out.printf("%-10s %-115s => %-70s%-70s%n", maps[0].getFreeSpace() + "/" + maps[1].getFreeSpace(), org, maps[0], maps[1]);
		}
	}


	private String s(int chr, int len)
	{
		return "---------------------------------------------------------------------------------------------".substring(0, len).replace('-', (char)chr);
	}
}
