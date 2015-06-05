package org.terifan.raccoon.io;

import java.io.IOException;
import java.util.Random;
import org.terifan.raccoon.util.Log;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class BlobInputStreamNGTest
{
	public BlobInputStreamNGTest()
	{
	}


	@Test
	public void testSomeMethod() throws IOException
	{
		Log.LEVEL = 10;

		Random rnd = new Random(1);

		byte[] out = new byte[1000];
		rnd.nextBytes(out);

		IPhysicalBlockDevice memoryDevice = new MemoryBlockDevice(512);
		IManagedBlockDevice blockDevice = new ManagedBlockDevice(memoryDevice);

		BlobOutputStream bos = new BlobOutputStream(blockDevice, 0);
		bos.write(out);
		bos.close();
		byte[] header = bos.getHeader();

		byte[] in = new byte[out.length];

		try (BlobInputStream bis = new BlobInputStream(blockDevice, header))
		{
			bis.read(in);
		}

//		Log.hexDump(out);
//		Log.out.println();
//		Log.hexDump(in);

		assertEquals(in, out);
	}
}
