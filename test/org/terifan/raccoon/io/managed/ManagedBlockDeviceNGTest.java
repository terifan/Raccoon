package org.terifan.raccoon.io.managed;

import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import java.io.IOException;
import org.terifan.raccoon.DatabaseException;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import static resources.__TestUtils.*;


public class ManagedBlockDeviceNGTest
{
	@Test
	public void testAllocationSimple() throws IOException
	{
		int s = 512;

		try (ManagedBlockDevice dev = new ManagedBlockDevice(new MemoryBlockDevice(s)))
		{
			long pos1 = dev.allocBlock(1);
			long pos2 = dev.allocBlock(1);
			dev.commit(); // allocs 2
			long pos3 = dev.allocBlock(1);
			long pos4 = dev.allocBlock(1);
			dev.commit(); // allocs 5, frees 2

			assertEquals(0, pos1);
			assertEquals(1, pos2);
			assertEquals(3, pos3);
			assertEquals(4, pos4);
		}
	}


	@Test
	public void testAllocationFreeSimple() throws IOException
	{
		int s = 512;

		try (ManagedBlockDevice dev = new ManagedBlockDevice(new MemoryBlockDevice(s)))
		{
			long pos1 = dev.allocBlock(1); // alloc 0
			long pos2 = dev.allocBlock(1); // alloc 1
			dev.commit(); // alloc 2
			long pos3 = dev.allocBlock(1); // alloc 3
			long pos4 = dev.allocBlock(1); // alloc 4
			dev.commit(); // alloc 5, free 2

			assertEquals(0, pos1);
			assertEquals(1, pos2);
			assertEquals(3, pos3);
			assertEquals(4, pos4);

			dev.freeBlock(0, 1); // free 0
			dev.freeBlock(1, 1); // free 1
			dev.commit(); // alloc 2, free 5

			long pos5 = dev.allocBlock(1); // alloc 0
			dev.commit(); // alloc 1, free 2

			assertEquals(0, pos5);
		}
	}


	@Test
	public void testMultiAllocationSimple() throws IOException
	{
		int s = 512;

		int rows = 250;
		long[] positions = new long[10 * rows];

		MemoryBlockDevice memoryBlockDevice = new MemoryBlockDevice(s);

		for (int test = 0; test < 10; test++)
		{
			try (ManagedBlockDevice dev = new ManagedBlockDevice(memoryBlockDevice))
			{
				if (test > 0)
				{
					byte[] buf = new byte[s];
					for (int i = 0; i < rows; i++)
					{
						dev.readBlock(positions[i], buf, 0, s, new int[4]);
						assertTrue(verifyRandomBuffer(i,buf));
					}
				}

				for (int i = test * rows; i < test * rows + rows; i++)
				{
					positions[i] = dev.allocBlock(1);
					dev.writeBlock(positions[i], createRandomBuffer(i,s), 0, s, new int[4]);
				}
				dev.commit();
			}
		}
	}


	@Test(expectedExceptions = DatabaseException.class)
	public void testDatabaseVersionConflict() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (IManagedBlockDevice blockDevice = new ManagedBlockDevice(device))
		{
			blockDevice.allocBlock(100);
			blockDevice.commit();
		}

		try (RaccoonDatabase db = new RaccoonDatabase(device, DatabaseOpenOption.OPEN, null)) // throws exception
		{
			fail();
		}
	}


//	@Test(expectedExceptions = UnsupportedVersionException.class)
//	public void testBadLabel() throws Exception
//	{
//		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//
//		ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice, "AnimalFarm");
//
//		try (Database db = new Database(managedBlockDevice, OpenOption.CREATE))
//		{
//		}
//
//		try (Database db = new Database(blockDevice, OpenOption.CREATE)) // default empty label won't match the "AnimalFarm" label causing an exception
//		{
//		}
//
//		fail();
//	}
}
