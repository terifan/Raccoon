package org.terifan.raccoon;

import java.io.IOException;
import org.terifan.raccoon.blockdevice.LobByteChannel;
import org.terifan.raccoon.blockdevice.LobOpenOption;
import org.terifan.raccoon.blockdevice.physical.MemoryBlockDevice;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.document.ObjectId;
import org.testng.annotations.Test;


public class RaccoonHeapNGTest
{
	@Test
	public void testSomeMethod() throws IOException
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.CREATE, null))
		{
			try (RaccoonHeap heap = db.getHeap("test"))
			{
//				heap.save(Document.of("hello:world"));
//				heap.save(Document.of("hello:world 12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"));
//
//				for (int i = 0; i < 1000; i++)
//				{
//					heap.save(Document.of("hello:world"));
//				}

				heap.put(100000, Document.of("hello:world"));
			}

			db.commit();
		}

//		blockDevice.dump();
	}
}
