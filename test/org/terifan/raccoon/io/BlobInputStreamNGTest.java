package org.terifan.raccoon.io;

import java.io.IOException;
import java.util.Random;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.testng.annotations.DataProvider;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class BlobInputStreamNGTest
{
	public BlobInputStreamNGTest()
	{
	}


	@Test(dataProvider = "unitSizes")
	public void testSomeMethod(int aUnitSize, int aPointers, boolean aIndirect) throws IOException
	{
//		Log.LEVEL = 10;

		Random rnd = new Random(1);

		byte[] out = new byte[aUnitSize];
		rnd.nextBytes(out);

		IPhysicalBlockDevice memoryDevice = new MemoryBlockDevice(512);
		SecureBlockDevice secureBlockDevice = new SecureBlockDevice(memoryDevice, new AccessCredentials("password"));
		IManagedBlockDevice blockDevice = new ManagedBlockDevice(secureBlockDevice);

		byte[] header;
		try (BlobOutputStream bos = new BlobOutputStream(blockDevice, 0))
		{
			bos.write(out);
			header = bos.finish();
		}
		
		if (aUnitSize == 0)
		{
			assertEquals(header.length, 1);
		}
		else
		{
			ByteArrayBuffer tmp = new ByteArrayBuffer(header);
			long len = tmp.readVar64();
			BlockPointer bp = new BlockPointer();

			assertEquals(len, aUnitSize);
			assertEquals(bp.unmarshal(tmp).getType(), aIndirect ? BlobOutputStream.TYPE_INDIRECT : BlobOutputStream.TYPE_DATA);
			assertEquals(tmp.remaining(), BlockPointer.SIZE * (aPointers - 1));
		}

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
	
	@DataProvider(name = "unitSizes")
	private Object[][] unitSizes()
	{
		return new Object[][]{
			{0, 1, false},
			{1000, 1, false},
			{1024*1024, 1, false},
			{4*1024*1024, 4, false},
			{10*1024*1024, 1, true}
		};
	}
}
