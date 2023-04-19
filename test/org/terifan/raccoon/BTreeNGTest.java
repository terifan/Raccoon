package org.terifan.raccoon;

import org.terifan.raccoon.document.Document;
import static org.terifan.raccoon.RaccoonCollection.TYPE_DOCUMENT;
import static org.terifan.raccoon._Tools.createSecureStorage;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.physical.MemoryBlockDevice;
import org.terifan.raccoon.blockdevice.physical.PhysicalBlockDevice;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class BTreeNGTest
{
	@Test
	public void testOpenCloseSecureBTree()
	{
//		Log.setLevel(LogLevel.DEBUG);

		PhysicalBlockDevice device = new MemoryBlockDevice(4096);

		ArrayMapKey key = new ArrayMapKey("key");
		byte[] value = "value".getBytes();

		try (BlockAccessor storage = createSecureStorage(device); BTree tree = new BTree(storage, new Document()))
		{
			tree.put(new ArrayMapEntry(key, value, TYPE_DOCUMENT));
			tree.commit();
			storage.getBlockDevice().getMetadata().put("conf", tree.getConfiguration());
			storage.getBlockDevice().commit();
		}

		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

		try (BlockAccessor storage = createSecureStorage(device); BTree tree = new BTree(storage, storage.getBlockDevice().getMetadata().getDocument("conf")))
		{
			ArrayMapEntry entry = new ArrayMapEntry(key);
			assertTrue(tree.get(entry));
			assertEquals(entry.getValue(), value);
			storage.getBlockDevice().commit();
		}
	}
}
