package org.terifan.raccoon;

import org.terifan.raccoon.exceptions.LobNotFoundException;
import java.util.Random;
import org.terifan.raccoon.blockdevice.lob.LobByteChannel;
import org.terifan.raccoon.blockdevice.lob.LobOpenOption;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.document.ObjectId;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import org.testng.annotations.Test;


public class RaccoonDirectoryNGTest
{
	@Test
	public void testCreateRead0() throws Exception
	{
		RaccoonBuilder builder = new RaccoonBuilder().withTarget(new MemoryBlockStorage(512));
		try (RaccoonDatabase db = builder.get())
		{
			db.getCollection("data").saveOne(Document.of("value:1"));
		}
		try (RaccoonDatabase db = builder.get())
		{
			db.getCollection("data").forEach(System.out::println);
		}
	}


	@Test
	public void testCreateRead() throws Exception
	{
		byte[] output = new byte[16000];
		new Random().nextBytes(output);
		Object id = ObjectId.randomId();

		RaccoonBuilder builder = new RaccoonBuilder().withTarget(new MemoryBlockStorage(512));

		try (RaccoonDatabase db = builder.get())
		{
			RaccoonDirectory dir = db.getDirectory("dir");

			try (LobByteChannel lob = dir.open(id, LobOpenOption.CREATE))
			{
				lob.writeAllBytes(output);
				lob.getMetadata().put("test", "abc");
			}
		}

		try (RaccoonDatabase db = builder.get())
		{
			RaccoonDirectory dir = db.getDirectory("dir");

			System.out.println(id);

			System.out.println(dir.list());

			try (LobByteChannel lob = dir.open(id, LobOpenOption.READ))
			{
				System.out.println(lob.getMetadata());
				byte[] input = lob.readAllBytes();
				assertEquals(input, output);
			}

//			dir.delete(id);
			System.out.println(dir.list());
		}
	}


	@Test(expectedExceptions = LobNotFoundException.class)
	public void testOpenMissing() throws Exception
	{
		Object id = ObjectId.randomId();

		MemoryBlockStorage blockDevice = new MemoryBlockStorage(512);

		try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.CREATE, null))
		{
			RaccoonDirectory dir = db.getDirectory("dir");
			try (LobByteChannel lob = dir.open(id, LobOpenOption.READ))
			{
				fail();
			}
		}
	}


	@Test(expectedExceptions = LobNotFoundException.class)
	public void testOpenDeleted() throws Exception
	{
		Object id = ObjectId.randomId();

		MemoryBlockStorage blockDevice = new MemoryBlockStorage(512);

		try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.CREATE, null))
		{
			RaccoonDirectory dir = db.getDirectory("dir");
			try (LobByteChannel lob = dir.open(id, LobOpenOption.READ))
			{
				lob.writeAllBytes(new byte[100]);
			}

			dir.delete(id);
			db.commit();
		}

		try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.OPEN, null))
		{
			RaccoonDirectory dir = db.getDirectory("dir");
			try (LobByteChannel lob = dir.open(id, LobOpenOption.READ))
			{
				fail();
			}
		}
	}

//	@Test
//	public void testConcurrentReadWriteLob() throws Exception
//	{
//		for (int test = 0; test < 1000; test++)
//		{
//			System.out.println(test);
//
//			MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//			Object[][] content = new Object[10000][];
//			for (int i = 0; i < content.length; i++)
//			{
//				content[i] = new Object[]{Integer.toString(i), createRandomBuffer(i, 1024)};
//			}
//
//			try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//			{
//				Stream.of(content).parallel().forEach(b ->
//				{
//					String key = (String)b[0];
//					byte[] buf = (byte[])b[1];
//					try (LobByteChannel channel = database.openLob(new _BlobKey1K(key), LobOpenOption.REPLACE))
//					{
//						channel.writeAllBytes(buf);
//					}
//					catch (Exception e)
//					{
//						throw new RuntimeException(e);
//					}
//				});
//				database.commit();
//			}
//
//			try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//			{
//				Stream.of(content).parallel().forEach(b ->
//				{
//					String key = (String)b[0];
//					byte[] buf = (byte[])b[1];
//					try (LobByteChannel channel = database.openLob(new _BlobKey1K(key), LobOpenOption.READ))
//					{
//						assertEquals(channel.readAllBytes(), buf);
//					}
//					catch (Exception e)
//					{
//						throw new RuntimeException(e);
//					}
//				});
//			}
//		}
//
////		try (Database database = new Database(device, DatabaseOpenOption.CREATE_NEW))
////		{
////			try (FixedThreadExecutor ex = new FixedThreadExecutor(8))
////			{
////				for (int i = 0; i < content.length; i++)
////				{
////					int _i = i;
////					ex.submit(()->
////					{
////						try (LobByteChannel channel = database.openLob(new _BlobKey1K(content[_i].toString()), LobOpenOption.REPLACE))
////						{
////							channel.writeAllBytes(content[_i]);
////						}
////						catch (Exception e)
////						{
////							throw new RuntimeException(e);
////						}
////					}
////					);
////				}
////			}
////			database.commit();
////		}
////
////		try (Database database = new Database(device, DatabaseOpenOption.OPEN))
////		{
////			try (FixedThreadExecutor ex = new FixedThreadExecutor(8))
////			{
////				for (int i = 0; i < content.length; i++)
////				{
////					int _i = i;
////					ex.submit(()->
////					{
////						try (LobByteChannel channel = database.openLob(new _BlobKey1K(content[_i].toString()), LobOpenOption.READ))
////						{
////							assertEquals(channel.readAllBytes(), content[_i]);
////						}
////						catch (Exception e)
////						{
////							throw new RuntimeException(e);
////						}
////					}
////					);
////				}
////			}
////		}
//	}
//
//
//	@Test
//	public void testExternalizedEntrySave() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//		byte[] content = createRandomBuffer(0, 10 * 1024 * 1024);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			database.save(new _KeyValue1K("my blob", content));
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			_KeyValue1K blob = new _KeyValue1K("my blob");
//			assertTrue(database.tryGet(blob));
//			assertEquals(blob._name, "my blob");
//			assertEquals(blob.content, content);
//		}
//	}
//
//
//	@Test
//	public void testExternalizedEntryDelete() throws IOException
//	{
//		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//		ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice);
//
//		_KeyValue1K in = new _KeyValue1K("apple", createRandomBuffer(0, 1000_000));
//		_KeyValue1K out = new _KeyValue1K("apple");
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.CREATE_NEW, CompressionParam.BEST_COMPRESSION))
//		{
//			assertTrue(db.save(in));
//			assertTrue(db.commit());
//		}
//
////		System.out.println(managedBlockDevice.getUsedSpace());
////		System.out.println(managedBlockDevice.getFreeSpace());
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.OPEN))
//		{
//			assertTrue(db.tryGet(out));
//
//			assertTrue(db.remove(out));
//			assertTrue(db.commit());
//		}
//
////		System.out.println(managedBlockDevice.getUsedSpace());
////		System.out.println(managedBlockDevice.getFreeSpace());
//		assertEquals(out.content, in.content);
//		assertEquals(managedBlockDevice.getUsedSpace(), 7);
//		assertTrue(managedBlockDevice.getFreeSpace() > 1000_000 / 512);
//	}
//
//
//	@Test
//	public void testExternalizedEntryUpdate() throws IOException
//	{
//		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//		ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice);
//
//		_KeyValue1K in = new _KeyValue1K("apple", createRandomBuffer(0, 1000_000));
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.CREATE_NEW, CompressionParam.BEST_COMPRESSION))
//		{
//			db.save(in);
//			db.commit();
//		}
//
//		in.content = null;
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.OPEN))
//		{
//			db.save(in);
//			db.commit();
//		}
//
//		assertEquals(managedBlockDevice.getUsedSpace(), 7);
//		assertTrue(managedBlockDevice.getFreeSpace() > 1000_000 / 512);
//	}
//
//
//	@Test
//	public void testReadNonExistingTable() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			try (LobByteChannel blob = database.openLob(new _BlobKey1K("test"), LobOpenOption.READ))
//			{
//				assertEquals(blob, null);
//			}
//			database.commit();
//		}
//	}
//
//
//	@Test
//	public void testReadNonExistingExternalizedEntry() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			assertEquals(database.size(new DiscriminatorType(new _BlobKey1K("apple"))), 0);
//			assertEquals(database.openLob(new _BlobKey1K("bad"), LobOpenOption.READ), null);
//			database.commit();
//		}
//	}
//
//
//	@Test(expectedExceptions = DatabaseException.class)
//	public void testDataCorruption() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		byte[] out = __TestUtils.createRandomBuffer(0, 1000_000);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW, CompressionParam.NO_COMPRESSION))
//		{
//			try (LobByteChannel blob = database.openLob(new _BlobKey1K("good"), LobOpenOption.CREATE))
//			{
//				blob.writeAllBytes(out);
//			}
//			database.commit();
//		}
//
//		byte[] buffer = new byte[512];
//		device.readBlock(300, buffer, 0, buffer.length, new long[2]);
//		buffer[0] ^= 1;
//		device.writeBlock(300, buffer, 0, buffer.length, new long[2]);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			try (LobByteChannel blob = database.openLob(new _BlobKey1K("good"), LobOpenOption.READ))
//			{
//				byte[] in = blob.readAllBytes();
//
//				assertEquals(in, out);
//			}
//		}
//	}
//
//
//	@Test
//	public void testInputStream() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			byte[] out = __TestUtils.createRandomBuffer(0, 10_000_000);
//
//			try (LobByteChannel blob = database.openLob(new _BlobKey1K("good"), LobOpenOption.CREATE))
//			{
//				blob.writeAllBytes(out);
//			}
//
//			try (InputStream blob = database.openLob(new _BlobKey1K("good"), LobOpenOption.READ).newInputStream())
//			{
//				ByteArrayOutputStream baos = new ByteArrayOutputStream();
//				Random rnd = new Random(1);
//				byte[] buf = new byte[2_000_000];
//				for (;;)
//				{
//					if (rnd.nextBoolean())
//					{
//						int c = blob.read();
//						if (c != -1)
//						{
//							baos.write(c);
//						}
//					}
//					else
//					{
//						int len = blob.read(buf, 0, rnd.nextInt(buf.length));
//						if (len <= 0)
//						{
//							break;
//						}
//						baos.write(buf, 0, len);
//					}
//				}
//
//				assertEquals(baos.toByteArray(), out);
//			}
//
//			database.commit();
//		}
//	}
//
//
//	@Test
//	public void testOutputStream() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			byte[] out = __TestUtils.createRandomBuffer(0, 10_000_000);
//
//			try (OutputStream blob = database.openLob(new _BlobKey1K("good"), LobOpenOption.CREATE).newOutputStream())
//			{
//				ByteArrayInputStream bais = new ByteArrayInputStream(out);
//				Random rnd = new Random(1);
//				byte[] buf = new byte[2_000_000];
//				for (;;)
//				{
//					if (rnd.nextBoolean())
//					{
//						int c = bais.read();
//						if (c != -1)
//						{
//							blob.write(c);
//						}
//					}
//					else
//					{
//						int len = bais.read(buf, 0, rnd.nextInt(buf.length));
//						if (len <= 0)
//						{
//							break;
//						}
//						blob.write(buf, 0, len);
//					}
//				}
//			}
//
//			try (LobByteChannel blob = database.openLob(new _BlobKey1K("good"), LobOpenOption.READ))
//			{
//				byte[] in = blob.readAllBytes();
//
//				assertEquals(in, out);
//			}
//
//			database.commit();
//		}
//	}
}
