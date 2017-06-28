package org.terifan.raccoon.io.managed;

import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import static resources.__TestUtils.createRandomBuffer;
import static resources.__TestUtils.verifyRandomBuffer;


public class LazyWriteCacheNGTest
{
	@Test
	public void testReadWriteBlock() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);
		LazyWriteCache cache = new LazyWriteCache(device, 4);
		for (int i = 0; i < 10; i++)
		{
			cache.writeBlock(i, createRandomBuffer(i, 512), 0, 512, new long[2]);
		}

		assertEquals(6, device.length()); // cached blocks are not yet written
		
		byte[] buffer = new byte[512];
		for (int i = 0; i < 10; i++)
		{
			cache.readBlock(i, buffer, 0, 512, new long[2]);
			assertTrue(verifyRandomBuffer(i, buffer));
		}

		assertEquals(6, device.length());

		cache.flush();
		
		assertEquals(10, device.length());
	}
}
