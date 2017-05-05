package org.terifan.raccoon.btree;

import java.io.IOException;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.TableParam;
import org.terifan.raccoon.TransactionCounter;
import org.terifan.raccoon.core.ArrayMap;
import org.terifan.raccoon.core.RecordEntry;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.testng.annotations.Test;
import static resources.__TestUtils.*;
import static org.testng.Assert.*;


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
	public void testSplitLeaf() throws IOException
	{
		int S = 60;

//		for (String ins : new String[]{"000","aaa","bbb","ccc","xxx"})
		for (String ins : new String[]{"aaa"})
		{
			for (int j = 0; j < 4; j++)
			{
				for (int i = 0; i < 23; i++)
				{
					ArrayMap map = new ArrayMap(new byte[S]);
					for (int k = 0; k < 4; k++)
					{
//						String key = Character.toString((char)('a' + k))+(j == k ? s(i) : "");
						String key = Character.toString((char)('a' + k));
						map.put(new RecordEntry(key.getBytes(), "123".getBytes(), (byte)0));
					}

					String org = map.toString();

					RecordEntry ne = new RecordEntry((ins + s(i)).getBytes(), "123".getBytes(), (byte)0);

					ArrayMap low = new ArrayMap(new byte[S]);
					ArrayMap high = new ArrayMap(new byte[S]);

					ArrayMap middle = BTree.splitLeafImpl2(map, low, high, S, ne);

					System.out.printf("%-10s %-60s => %s%n", low.getFreeSpace()+"/"+(middle==null?"":middle.getFreeSpace())+"/"+high.getFreeSpace(), org, low+" "+middle+" "+high);
				}
			}
		}
	}


	private String s(int len)
	{
		return "---------------------------------------------------------------------------------------------".substring(0,len);
	}
}
