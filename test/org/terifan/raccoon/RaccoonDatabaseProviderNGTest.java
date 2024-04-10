package org.terifan.raccoon;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.terifan.raccoon.blockdevice.BlockDeviceOpenOption;
import org.terifan.raccoon.blockdevice.RaccoonStorage;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.blockdevice.secure.CipherModeFunction;
import org.terifan.raccoon.blockdevice.secure.EncryptionFunction;
import org.terifan.raccoon.blockdevice.secure.SecureBlockDevice;
import org.terifan.raccoon.blockdevice.storage.FileBlockStorage;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import org.terifan.raccoon.blockdevice.util.PathUtils;
import org.terifan.raccoon.document.Document;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;


public class RaccoonDatabaseProviderNGTest
{
	@Test
	public void testStorageAllocation() throws Exception
	{
		Path path = PathUtils.produceUserHomePath("test.fbs");

		try
		{
			Random rnd = new Random(1);
			byte[] temp1 = new byte[4096];
			byte[] temp2 = new byte[4096];
			int[] iv =
			{
				rnd.nextInt(), rnd.nextInt(), rnd.nextInt(), rnd.nextInt()
			};

			Files.deleteIfExists(path);
			try (FileBlockStorage storage = new FileBlockStorage(path).open(BlockDeviceOpenOption.REPLACE))
			{
				rnd.nextBytes(temp1);
				storage.writeBlock(0, temp1, 0, temp1.length, null);
			}
			try (FileBlockStorage storage = new FileBlockStorage(path).open(BlockDeviceOpenOption.OPEN))
			{
				storage.readBlock(0, temp2, 0, temp1.length, null);
				assertEquals(temp2, temp1);
			}

			Files.deleteIfExists(path);
			try (SecureBlockDevice storage = new SecureBlockDevice(new AccessCredentials("pass"), new FileBlockStorage(path)).open(BlockDeviceOpenOption.REPLACE))
			{
				rnd.nextBytes(temp1);
				storage.writeBlock(0, temp1, 0, temp1.length, iv);
			}
			try (SecureBlockDevice storage = new SecureBlockDevice(new AccessCredentials("pass"), new FileBlockStorage(path)).open(BlockDeviceOpenOption.OPEN))
			{
				storage.readBlock(0, temp2, 0, temp1.length, iv);
				assertEquals(temp2, temp1);
			}

			MemoryBlockStorage memoryBlockStorage = new MemoryBlockStorage();
			try (MemoryBlockStorage storage = memoryBlockStorage.open(BlockDeviceOpenOption.REPLACE))
			{
				rnd.nextBytes(temp1);
				storage.writeBlock(0, temp1, 0, temp1.length, null);
			}
			try (MemoryBlockStorage storage = memoryBlockStorage.open(BlockDeviceOpenOption.OPEN))
			{
				storage.readBlock(0, temp2, 0, temp1.length, null);
				assertEquals(temp2, temp1);
			}

			RaccoonDatabaseProvider provider = new RaccoonDatabaseProvider(new RaccoonStorage().inMemory());
			try (RaccoonDatabase db = provider.get())
			{
				db.getCollection("data").saveOne(Document.of("_id:1"));
			}
			try (RaccoonDatabase db = provider.get())
			{
				db.getCollection("data").findOne(Document.of("_id:1")).get();
			}

			provider = new RaccoonDatabaseProvider(new RaccoonStorage().inTemporaryFile());
			try (RaccoonDatabase db = provider.get())
			{
				db.getCollection("data").saveOne(Document.of("_id:2"));
			}
			try (RaccoonDatabase db = provider.get())
			{
				db.getCollection("data").findOne(Document.of("_id:2")).get();
			}

			provider = new RaccoonDatabaseProvider(new RaccoonStorage().withPassword("pass").inMemory());
			try (RaccoonDatabase db = provider.get())
			{
				db.getCollection("data").saveOne(Document.of("_id:3"));
			}
			try (RaccoonDatabase db = provider.get())
			{
				db.getCollection("data").findOne(Document.of("_id:3")).get();
			}

			provider = new RaccoonDatabaseProvider(new RaccoonStorage().withPassword("pass").inTemporaryFile());
			try (RaccoonDatabase db = provider.get())
			{
				db.getCollection("data").saveOne(Document.of("_id:4"));
			}
			try (RaccoonDatabase db = provider.get())
			{
				db.getCollection("data").findOne(Document.of("_id:4")).get();
			}

			provider = new RaccoonDatabaseProvider(new RaccoonStorage().withPassword("pass").withCipherMode(CipherModeFunction.ELEPHANT).withEncryption(EncryptionFunction.KUZNECHIK_TWOFISH_AES).withKeyGenerationCost(1024, 32, 1, 1000).inMemory());
			try (RaccoonDatabase db = provider.get())
			{
				db.getCollection("data").saveOne(Document.of("_id:5"));
			}
			try (RaccoonDatabase db = provider.get())
			{
				db.getCollection("data").findOne(Document.of("_id:5")).get();
			}
		}
		finally
		{
			Files.deleteIfExists(path);
		}
	}
}
