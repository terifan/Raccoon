package org.terifan.raccoon;

import java.nio.file.Paths;
import org.terifan.raccoon.document.Document;
import static org.terifan.raccoon.RaccoonCollection.TYPE_DOCUMENT;
import static org.terifan.raccoon._Tools.createSecureStorage;
import org.terifan.raccoon.io.physical.FileBlockDevice;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class BTreeNGTest
{
	@Test
	public void testOpenCloseSecureBTree()
	{
//		Log.setLevel(LogLevel.DEBUG);

//		IPhysicalBlockDevice device = new MemoryBlockDevice(512);
		IPhysicalBlockDevice device = new FileBlockDevice(Paths.get("d:/test-" + System.currentTimeMillis() + ".rdb"));

		ArrayMapKey key = new ArrayMapKey("key");
		byte[] value = "value".getBytes();

		try (BlockAccessor storage = createSecureStorage(device); BTree tree = new BTree(storage, new Document()))
		{
			tree.put(new ArrayMapEntry(key, value, TYPE_DOCUMENT));
			tree.commit();
			storage.getBlockDevice().getApplicationMetadata().put("conf", tree.getConfiguration());
			storage.getBlockDevice().commit();
		}

		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

		try (BlockAccessor storage = createSecureStorage(device); BTree tree = new BTree(storage, storage.getBlockDevice().getApplicationMetadata().getDocument("conf")))
		{
			ArrayMapEntry entry = new ArrayMapEntry(key);
			assertTrue(tree.get(entry));
			assertEquals(entry.getValue(), value);
			storage.getBlockDevice().commit();
		}
	}
}
