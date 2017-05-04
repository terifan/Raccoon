package org.terifan.raccoon.storage;

import org.terifan.raccoon.core.BlockType;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.CompressionParam;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;
import org.testng.annotations.DataProvider;


public class BlockAccessorNGTest
{
	@Test
	public void testWriteReadFreeSingleBlock() throws IOException
	{
		int length = 3 * 512;
		byte[] in = new byte[100 + length + 100];
		new Random().nextBytes(in);

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice, "", 512);
		BlockAccessor blockAccessor = new BlockAccessor(managedBlockDevice, CompressionParam.NO_COMPRESSION, 0);
		BlockPointer blockPointer = blockAccessor.writeBlock(in, 100, length, 0L, BlockType.FREE, 0);
		managedBlockDevice.commit();

		assertEquals(2 + 1 + 3, managedBlockDevice.getAllocatedSpace()); // 2 superblock + 1 spacemap + 3 data
		assertEquals(0, managedBlockDevice.getFreeSpace());

		byte[] out = blockAccessor.readBlock(blockPointer);
		byte[] expcted = Arrays.copyOfRange(in, 100, 100 + length);

		assertEquals(expcted, out);

		blockAccessor.freeBlock(blockPointer);
		managedBlockDevice.commit();

		assertEquals(2 + 2 + 3, managedBlockDevice.getAllocatedSpace()); // 2 superblock + 2 spacemap + 3 data
		assertEquals(1 + 3, managedBlockDevice.getFreeSpace()); // 1 spacemap + 3 data
	}


	@DataProvider
	private Object[][] cacheSize()
	{
		return new Object[][]{
			{false},
			{true}
		};
	}
}
