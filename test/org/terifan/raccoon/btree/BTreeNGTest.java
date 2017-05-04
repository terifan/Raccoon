package org.terifan.raccoon.btree;

import java.io.IOException;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.LogLevel;
import org.terifan.raccoon.TableParam;
import org.terifan.raccoon.TransactionCounter;
import org.terifan.raccoon.core.RecordEntry;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.util.Log;
import org.testng.annotations.Test;


public class BTreeNGTest
{
	@Test
	public void testSomeMethod() throws IOException
	{
		Log.mLevel = LogLevel.DEBUG;

		MemoryBlockDevice memoryBlockDevice = new MemoryBlockDevice(512);

		IManagedBlockDevice blockDevice = new ManagedBlockDevice(memoryBlockDevice, null, 0);

		BTree table = new BTree(blockDevice, null, new TransactionCounter(0), true, CompressionParam.NO_COMPRESSION, TableParam.DEFAULT);

		table.put(new RecordEntry("abc".getBytes(), "123".getBytes(), (byte)0));

		table.commit();

		memoryBlockDevice.dump();
	}
}
