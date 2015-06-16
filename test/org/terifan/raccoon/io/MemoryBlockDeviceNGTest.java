package org.terifan.raccoon.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import org.testng.annotations.Test;
import static org.testng.Assert.*;


public class MemoryBlockDeviceNGTest
{
	@Test
	public void testSomeMethod() throws IOException
	{
		Random rnd = new Random(1);
		int s = 512;
		HashMap<Long,byte[]> blocks = new HashMap<>();
		ArrayList<Long> offsets = new ArrayList<>();

		MemoryBlockDevice memoryBlockDevice = new MemoryBlockDevice(s);

		for (int k = 0; k < 5; k++)
		{
			try (ManagedBlockDevice dev = new ManagedBlockDevice(memoryBlockDevice))
			{
				for (int j = 0; j < 50; j++)
				{
					for (int i = 0; i < 200; i++)
					{
						long pos = dev.allocBlock(1);
						byte[] buf = new byte[s];
						rnd.nextBytes(buf);
						blocks.put(pos, buf);
						dev.writeBlock(pos, buf, 0, s, 0L);
						offsets.add(pos);
					}

					for (int i = 0; i < 100; i++)
					{
						long pos = offsets.remove(rnd.nextInt(offsets.size()));
						byte[] buf = new byte[s];
						dev.readBlock(pos, buf, 0, s, 0L);
						assertEquals(blocks.remove(pos), buf);
						dev.freeBlock(pos, 1);
					}

					dev.commit();
				}
			}
		}
	}
}
