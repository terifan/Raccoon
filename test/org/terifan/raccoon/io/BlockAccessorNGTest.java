package org.terifan.raccoon.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.io.BlockPointer.BlockType;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;
import org.testng.annotations.DataProvider;
import tests.__FixedThreadExecutor;


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

		BlockPointer blockPointer = blockAccessor.writeBlock(in, 100, length, 0L, BlockType.NODE_FREE, 0);
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


//	@Test(dataProvider = "deviceType")
//	public void testConcurrency(boolean aSecure) throws IOException
//	{
////for(int test=0; test<100; test++)
//{
//
//		int iterations = 1000;
//		int size = 100;
//		int offset = 100;
//		int length = size * 512;
//
//		byte[][] in = new byte[iterations][offset + length + 100];
//		Random rnd = new Random(1);
//		for (int i = 0; i < iterations; i++)
//		{
//			rnd.nextBytes(in[i]);
//		}
//
//		MemoryBlockDevice memoryBlockDevice = new MemoryBlockDevice(512);
//
//		IPhysicalBlockDevice secureBlockDevice = !aSecure ? memoryBlockDevice : new SecureBlockDevice(memoryBlockDevice, new AccessCredentials("password"));
//
//		ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(secureBlockDevice);
//		BlockAccessor blockAccessor = new BlockAccessor(managedBlockDevice);
//
//		BlockPointer[] blockPointer = new BlockPointer[iterations];
//
//		try (__FixedThreadExecutor executor = new __FixedThreadExecutor(1f))
//		{
//			for (int j = 0; j < iterations; j++)
//			{
//				int i = j;
//				executor.submit(new Runnable()
//				{
//					@Override
//					public void run()
//					{
//						blockPointer[i] = blockAccessor.writeBlock(in[i], offset, length, 0L, BlockType.NODE_FREE, 0);
//					}
//				});
//			}
//		}
//
//		managedBlockDevice.commit();
//
//		try (__FixedThreadExecutor executor = new __FixedThreadExecutor(1f))
//		{
//			for (int j = 0; j < iterations; j++)
//			{
//				int i = j;
//				executor.submit(new Runnable()
//				{
//					@Override
//					public void run()
//					{
//						byte[] out = blockAccessor.readBlock(blockPointer[i]);
//						byte[] expected = Arrays.copyOfRange(in[i], offset, 100 + length);
//
//						assertEquals(expected, out);
//					}
//				});
//			}
//		}
//
//}
//	}


	@DataProvider
	private Object[][] deviceType()
	{
		return new Object[][]{
			{false},
			{true}
		};
	}
}
