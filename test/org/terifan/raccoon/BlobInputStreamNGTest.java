package org.terifan.raccoon;

import java.io.IOException;
import java.util.Random;
import org.terifan.raccoon.TransactionCounter;
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.io.BlobInputStream;
import org.terifan.raccoon.io.BlobOutputStream;
import org.terifan.raccoon.io.BlockAccessor;
import org.terifan.raccoon.io.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.testng.annotations.DataProvider;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import org.terifan.raccoon.io.BlockPointer.BlockType;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.io.IPhysicalBlockDevice;
import org.terifan.raccoon.io.ManagedBlockDevice;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.io.SecureBlockDevice;


public class BlobInputStreamNGTest
{
	@Test(dataProvider = "unitSizes")
	public void testReadWriteBlob(int aUnitSize, int aPointers, boolean aIndirect) throws IOException
	{
//		Log.LEVEL = 10;

		Random rnd = new Random(1);

		byte[] out = new byte[aUnitSize];
		rnd.nextBytes(out);

		IPhysicalBlockDevice memoryDevice = new MemoryBlockDevice(512);
		SecureBlockDevice secureBlockDevice = new SecureBlockDevice(memoryDevice, new AccessCredentials("password"));
		IManagedBlockDevice blockDevice = new ManagedBlockDevice(secureBlockDevice);

		byte[] header;
		try (BlobOutputStream bos = new BlobOutputStream(new BlockAccessor(blockDevice), new TransactionCounter(0)))
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
			assertEquals(bp.unmarshal(tmp).getType(), aIndirect ? BlockType.BLOB_INDX : BlockType.BLOB_DATA);
			assertEquals(tmp.remaining(), BlockPointer.SIZE * (aPointers - 1));
		}

		byte[] in = new byte[out.length];

		try (BlobInputStream bis = new BlobInputStream(new BlockAccessor(blockDevice), header))
		{
			bis.read(in);
		}

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
