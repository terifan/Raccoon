package org.terifan.raccoon;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.stream.Stream;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import resources.__TestUtils;
import static resources.__TestUtils.createRandomBuffer;
import resources.entities._BlobKey1K;
import resources.entities._KeyValue1K;


public class LobTableNGTest
{
	@Test
	public void testConcurrentReadWriteLob() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);
		byte[][] content = new byte[1000][];
		for (int i = 0; i < content.length; i++)
		{
			content[i] = createRandomBuffer(i, 1024);
		}

		try (Database database = new Database(device, DatabaseOpenOption.CREATE_NEW))
		{
			Stream.of(content).parallel().forEach(b ->
			{
				try (LobByteChannel channel = database.openLob(new _BlobKey1K(b.toString()), LobOpenOption.REPLACE))
				{
					channel.writeAllBytes(b);
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			});
			database.commit();
		}

		try (Database database = new Database(device, DatabaseOpenOption.OPEN))
		{
			Stream.of(content).parallel().forEach(b ->
			{
				try (LobByteChannel channel = database.openLob(new _BlobKey1K(b.toString()), LobOpenOption.READ))
				{
					assertEquals(channel.readAllBytes(), b);
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			});
		}
	}


	@Test
	public void testExternalizedEntrySave() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);
		byte[] content = createRandomBuffer(0, 10 * 1024 * 1024);

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
			assertTrue(db.save(in));
			assertTrue(db.commit());
		}

//		System.out.println(managedBlockDevice.getUsedSpace());
//		System.out.println(managedBlockDevice.getFreeSpace());
		try (Database db = new Database(managedBlockDevice, DatabaseOpenOption.OPEN))
		{
			assertTrue(db.tryGet(out));

			assertTrue(db.remove(out));
			assertTrue(db.commit());
		}

//		System.out.println(managedBlockDevice.getUsedSpace());
//		System.out.println(managedBlockDevice.getFreeSpace());
		assertEquals(out.content, in.content);
		assertEquals(managedBlockDevice.getUsedSpace(), 7);
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

		assertEquals(managedBlockDevice.getUsedSpace(), 7);
		assertTrue(managedBlockDevice.getFreeSpace() > 1000_000 / 512);
	}


	@Test
	public void testReadNonExistingTable() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = new Database(device, DatabaseOpenOption.CREATE_NEW))
		{
			try (LobByteChannel blob = database.openLob(new _BlobKey1K("test"), LobOpenOption.READ))
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
			assertEquals(database.openLob(new _BlobKey1K("bad"), LobOpenOption.READ), null);
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
			try (LobByteChannel blob = database.openLob(new _BlobKey1K("good"), LobOpenOption.CREATE))
			{
				blob.writeAllBytes(out);
			}
			database.commit();
		}

		byte[] buffer = new byte[512];
		device.readBlock(300, buffer, 0, buffer.length, new long[2]);
		buffer[0] ^= 1;
		device.writeBlock(300, buffer, 0, buffer.length, new long[2]);

		try (Database database = new Database(device, DatabaseOpenOption.OPEN))
		{
			try (LobByteChannel blob = database.openLob(new _BlobKey1K("good"), LobOpenOption.READ))
			{
				byte[] in = blob.readAllBytes();

				assertEquals(in, out);
			}
		}
	}


	@Test
	public void testInputStream() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = new Database(device, DatabaseOpenOption.CREATE_NEW))
		{
			byte[] out = __TestUtils.createRandomBuffer(0, 10_000_000);

			try (LobByteChannel blob = database.openLob(new _BlobKey1K("good"), LobOpenOption.CREATE))
			{
				blob.writeAllBytes(out);
			}

			try (InputStream blob = database.openLob(new _BlobKey1K("good"), LobOpenOption.READ).newInputStream())
			{
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				Random rnd = new Random(1);
				byte[] buf = new byte[2_000_000];
				for (;;)
				{
					if (rnd.nextBoolean())
					{
						int c = blob.read();
						if (c != -1)
						{
							baos.write(c);
						}
					}
					else
					{
						int len = blob.read(buf, 0, rnd.nextInt(buf.length));
						if (len <= 0)
						{
							break;
						}
						baos.write(buf, 0, len);
					}
				}

				assertEquals(baos.toByteArray(), out);
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
			byte[] out = __TestUtils.createRandomBuffer(0, 10_000_000);

			try (OutputStream blob = database.openLob(new _BlobKey1K("good"), LobOpenOption.CREATE).newOutputStream())
			{
				ByteArrayInputStream bais = new ByteArrayInputStream(out);
				Random rnd = new Random(1);
				byte[] buf = new byte[2_000_000];
				for (;;)
				{
					if (rnd.nextBoolean())
					{
						int c = bais.read();
						if (c != -1)
						{
							blob.write(c);
						}
					}
					else
					{
						int len = bais.read(buf, 0, rnd.nextInt(buf.length));
						if (len <= 0)
						{
							break;
						}
						blob.write(buf, 0, len);
					}
				}
			}

			try (LobByteChannel blob = database.openLob(new _BlobKey1K("good"), LobOpenOption.READ))
			{
				byte[] in = blob.readAllBytes();

				assertEquals(in, out);
			}

			database.commit();
		}
	}
}
