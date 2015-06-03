package org.terifan.raccoon.io;

import java.io.IOException;
import java.util.Random;
import org.terifan.raccoon.util.Log;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class BlobOutputStreamNGTest
{
	public BlobOutputStreamNGTest()
	{
	}


	@Test
	public void testSomeMethod() throws IOException
	{
		Log.LEVEL = 10;

		Random rnd = new Random(1);

		byte[] buffer = new byte[100000000];
		rnd.nextBytes(buffer);

		IPhysicalBlockDevice memoryDevice = new MemoryBlockDevice(512);
		IManagedBlockDevice blockDevice = new ManagedBlockDevice(memoryDevice);

		BlobOutputStream bos = new BlobOutputStream(blockDevice, 0);
		bos.write(buffer);
		bos.close();

		Log.hexDump(bos.getHeader());

		assertTrue(true);
	}
}
