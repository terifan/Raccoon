package org.terifan.raccoon;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.util.Log;
import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import resources.__TestUtils;
import static resources.__TestUtils.createRandomBuffer;
import resources.entities._BlobKey1K;
import resources.entities._KeyValue1K;


public class BlobNGTest
{
	@Test
	public void testSomeMethod()
	{
	}


	@Test
	public void testExternalizedEntrySave() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);
		byte[] content = createRandomBuffer(0, 10*1024*1024);

		try (Database database = new Database(device, DatabaseOpenOption.CREATE_NEW))
		{
			database.save(new _KeyValue1K("my blob", content));
			database.commit();
		}

		try (Database database = new Database(device, DatabaseOpenOption.OPEN))
		{
			_KeyValue1K blob = new _KeyValue1K("my blob");
			assertTrue(database.tryGet(blob));
			assertEquals(blob._name, "my blob");
			assertEquals(blob.content, content);
		}
	}


	@Test
	public void testExternalizedEntryDelete() throws IOException
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
		ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice);

		_KeyValue1K in = new _KeyValue1K("apple", createRandomBuffer(0, 1000_000));
		_KeyValue1K out = new _KeyValue1K("apple");

		try (Database db = new Database(managedBlockDevice, DatabaseOpenOption.CREATE_NEW, CompressionParam.BEST_COMPRESSION))
		{
			db.save(in);
			db.commit();
		}

		try (Database db = new Database(managedBlockDevice, DatabaseOpenOption.OPEN))
		{
			db.tryGet(out);

			db.remove(out);
			db.commit();
		}

		assertEquals(out.content, in.content);
		assertEquals(managedBlockDevice.getUsedSpace(), 5);
		assertTrue(managedBlockDevice.getFreeSpace() > 1000_000 / 512);
	}


	@Test
	public void testExternalizedEntryUpdate() throws IOException
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
		ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice);

		_KeyValue1K in = new _KeyValue1K("apple", createRandomBuffer(0, 1000_000));

		try (Database db = new Database(managedBlockDevice, DatabaseOpenOption.CREATE_NEW, CompressionParam.BEST_COMPRESSION))
		{
			db.save(in);
			db.commit();
		}

		in.content = null;

		try (Database db = new Database(managedBlockDevice, DatabaseOpenOption.OPEN))
		{
			db.save(in);
			db.commit();
		}

		assertEquals(managedBlockDevice.getUsedSpace(), 5);
		assertTrue(managedBlockDevice.getFreeSpace() > 1000_000 / 512);
	}


	@Test
	public void testReadNonExistingTable() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = new Database(device, DatabaseOpenOption.CREATE_NEW))
		{
			try (Blob blob = database.openBlob(new _BlobKey1K("test"), BlobOpenOption.READ))
			{
				assertEquals(blob, null);
			}
			database.commit();
		}
	}


	@Test
	public void testReadNonExistingExternalizedEntry() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = new Database(device, DatabaseOpenOption.CREATE_NEW))
		{
			assertEquals(database.size(new DiscriminatorType(new _BlobKey1K("apple"))), 0);
			assertEquals(database.openBlob(new _BlobKey1K("bad"), BlobOpenOption.READ), null);
			database.commit();
		}
	}


	@Test(expectedExceptions = DatabaseException.class)
	public void testDataCorruption() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		byte[] out = __TestUtils.createRandomBuffer(0, 1000_000);

		try (Database database = new Database(device, DatabaseOpenOption.CREATE_NEW, CompressionParam.NO_COMPRESSION))
		{
			try (Blob blob = database.openBlob(new _BlobKey1K("good"), BlobOpenOption.CREATE))
			{
				blob.writeAllBytes(out);
			}
			database.commit();
		}

		byte[] buffer = new byte[512];
		device.readBlock(100, buffer, 0, buffer.length, new long[2]);
		buffer[0] ^= 1;
		device.writeBlock(100, buffer, 0, buffer.length, new long[2]);

		try (Database database = new Database(device, DatabaseOpenOption.OPEN))
		{
			try (Blob blob = database.openBlob(new _BlobKey1K("good"), BlobOpenOption.READ))
			{
				assertEquals(blob.readAllBytes(), out);
			}
		}
	}


	@Test
	public void testInputStream() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = new Database(device, DatabaseOpenOption.CREATE_NEW))
		{
			byte[] out = __TestUtils.createRandomBuffer(0, 10000);

			try (Blob blob = database.openBlob(new _BlobKey1K("good"), BlobOpenOption.CREATE))
			{
				blob.writeAllBytes(out);
			}

			try (InputStream is = database.openBlob(new _BlobKey1K("good"), BlobOpenOption.READ).newInputStream())
			{
				byte[] in = new byte[out.length];

				is.read(in, 0, 100);
				is.read(in, 100, 9800);
				in[9900] = (byte)is.read();
				is.read(in, 9901, 99);

				assertEquals(in, out);
			}

			database.commit();
		}
	}


	@Test
	public void testOutputStream() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = new Database(device, DatabaseOpenOption.CREATE_NEW))
		{
			byte[] out = __TestUtils.createRandomBuffer(0, 10000);

			try (OutputStream blob = database.openBlob(new _BlobKey1K("good"), BlobOpenOption.CREATE).newOutputStream())
			{
				blob.write(out, 0, 100);
				blob.write(out, 100, 9800);
				blob.write(0xff & out[9900]);
				blob.write(out, 9901, 99);
			}

			try (Blob in = database.openBlob(new _BlobKey1K("good"), BlobOpenOption.READ))
			{
				byte[] buf = in.readAllBytes();

				assertEquals(buf, out);
			}

			database.commit();
		}
	}


//	@Test
//	public void testBlobCompressionParameter() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (Database database = new Database(device, OpenOption.CREATE_NEW, new CompressionParam(CompressionParam.NONE, CompressionParam.NONE, CompressionParam.DEFLATE_BEST)))
//		{
//			database.save(new _BlobKey1K("good"), new ByteArrayInputStream(new byte[1000_000]));
//			database.commit();
//		}
//
//		assertTrue(device.length() > 10);
//		assertTrue(device.length() < 40);
//	}
//
//
//	@Test
//	public void testNodeCompressionParameter() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (Database database = new Database(device, OpenOption.CREATE_NEW, new CompressionParam(CompressionParam.NONE, CompressionParam.DEFLATE_BEST, CompressionParam.DEFLATE_BEST)))
//		{
//			database.save(new _BlobKey1K("good"), new ByteArrayInputStream(new byte[1000_000]));
//			database.commit();
//		}
//
//		assertTrue(device.length() < 10);
//	}
//
//
//	@Test
//	public void testSaveBlob() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (Database database = new Database(device, OpenOption.CREATE_NEW))
//		{
//			try (BlobOutputStream bos = database.saveBlob(new _BlobKey1K("test")))
//			{
//				bos.write(new byte[1000_000]);
//			}
//
//			database.commit();
//		}
//	}


//	@Test(dataProvider = "unitSizes")
//	public void testReadWriteBlob(int aUnitSize, int aPointers, boolean aIndirect) throws IOException
//	{
////		Log.LEVEL = 10;
//
//		Random rnd = new Random(1);
//
//		byte[] out = new byte[aUnitSize];
//		rnd.nextBytes(out);
//
//		IPhysicalBlockDevice memoryDevice = new MemoryBlockDevice(512);
//		SecureBlockDevice secureBlockDevice = SecureBlockDevice.create(new AccessCredentials("password").setIterationCount(1), memoryDevice);
//		IManagedBlockDevice blockDevice = new ManagedBlockDevice(secureBlockDevice);
//
//		byte[] header;
//		try (BlobOutputStream bos = new BlobOutputStream(new BlockAccessor(blockDevice, CompressionParam.BEST_SPEED, 0), new TransactionGroup(0), null))
//		{
//			bos.write(out);
//			header = bos.finish();
//		}
//
//		if (aUnitSize == 0)
//		{
//			assertEquals(header.length, 1);
//		}
//		else
//		{
//			ByteArrayBuffer tmp = new ByteArrayBuffer(header);
//			long len = tmp.readVar64();
//			BlockPointer bp = new BlockPointer();
//
//			assertEquals(len, aUnitSize);
//			assertEquals(bp.unmarshal(tmp).getBlockType(), aIndirect ? BlockType.BLOB_INDEX : BlockType.BLOB_DATA);
//			assertEquals(tmp.remaining(), BlockPointer.SIZE * (aPointers - 1));
//		}
//
//		byte[] in = new byte[out.length];
//
//		try (BlobInputStream bis = new BlobInputStream(new BlockAccessor(blockDevice, CompressionParam.BEST_SPEED, 0), header))
//		{
//			bis.read(in);
//		}
//
//		assertEquals(in, out);
//	}

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
