package org.terifan.raccoon;

import java.util.Random;
import org.terifan.raccoon.blockdevice.LobByteChannelOld;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import org.terifan.raccoon.document.ObjectId;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import org.testng.annotations.Test;


public class RaccoonDirectoryNGTest
{
	@Test
	public void testCreateRead() throws Exception
	{
		byte[] output = new byte[16000];
		new Random().nextBytes(output);
		Object id = ObjectId.randomId();

		MemoryBlockStorage blockDevice = new MemoryBlockStorage(512);

		try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.CREATE, null))
		{
			RaccoonDirectory dir = db.getDirectory("dir");
			try (LobByteChannelOld lob = dir.open(id, LobOpenOption.CREATE))
			{
				lob.writeAllBytes(output);
				lob.getMetadata().put("test", "abc");
			}

			db.commit();
		}

		try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.OPEN, null))
		{
			RaccoonDirectory dir = db.getDirectory("dir");
			try (LobByteChannelOld lob = dir.open(id, LobOpenOption.READ))
			{
				System.out.println(lob.getMetadata());
				byte[] input = lob.readAllBytes();
				assertEquals(input, output);
			}

			dir.delete(id);

			System.out.println(dir.list());

			db.commit();
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
			try (LobByteChannelOld lob = dir.open(id, LobOpenOption.READ))
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
			try (LobByteChannelOld lob = dir.open(id, LobOpenOption.READ))
			{
				lob.writeAllBytes(new byte[100]);
			}

			dir.delete(id);
			db.commit();
		}

		try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.OPEN, null))
		{
			RaccoonDirectory dir = db.getDirectory("dir");
			try (LobByteChannelOld lob = dir.open(id, LobOpenOption.READ))
			{
				fail();
			}
		}
	}
}
