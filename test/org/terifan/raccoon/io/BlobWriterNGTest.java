package org.terifan.raccoon.io;

import org.terifan.raccoon.io.BlobWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.io.IPhysicalBlockDevice;
import org.terifan.raccoon.io.ManagedBlockDevice;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.util.Log;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class BlobWriterNGTest
{
	public BlobWriterNGTest()
	{
	}


	@Test
	public void testSomeMethod() throws IOException
	{
		Log.LEVEL = 10;
		
		Random rnd = new Random(1);

		byte[] buffer = new byte[1000000];
		rnd.nextBytes(buffer);
		
		IPhysicalBlockDevice memoryDevice = new MemoryBlockDevice(512);
		IManagedBlockDevice blockDevice = new ManagedBlockDevice(memoryDevice);

		byte[] header = BlobWriter.transfer(blockDevice, 0, new ByteArrayInputStream(buffer));

		Log.hexDump(header);

		assertTrue(true);
	}
}
