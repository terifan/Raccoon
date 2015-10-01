package org.terifan.raccoon.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.TransactionId;
import org.terifan.raccoon.util.Log;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;


public class BlockAccessorNGTest
{
	@Test
	public void testWriteReadFreeSingleBlock() throws IOException
	{
		int length = 3 * 512;
		byte[] in = new byte[100 + length + 100];
		new Random().nextBytes(in);

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
		ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice);
		BlockAccessor blockAccessor = new BlockAccessor(managedBlockDevice);

		BlockPointer blockPointer = blockAccessor.writeBlock(in, 100, length, 0L, 0, 0);
		managedBlockDevice.commit();

		assertEquals(2 + 1 + 3, managedBlockDevice.getAllocatedSpace()); // 2 superblock + 1 spacemap + 3 data
		assertEquals(0, managedBlockDevice.getFreeSpace());

		byte[] out = blockAccessor.readBlock(blockPointer);

		assertEquals(out, Arrays.copyOfRange(in, 100, 100 + length));

		blockAccessor.freeBlock(blockPointer);
		managedBlockDevice.commit();

		assertEquals(2 + 2 + 3, managedBlockDevice.getAllocatedSpace()); // 2 superblock + 2 spacemap + 3 data
		assertEquals(1 + 3, managedBlockDevice.getFreeSpace()); // 1 spacemap + 3 data
	}
}
