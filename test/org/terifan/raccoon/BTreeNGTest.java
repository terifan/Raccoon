package org.terifan.raccoon;

import java.util.function.Supplier;
import org.terifan.raccoon.document.Document;
import static org.terifan.raccoon.RaccoonCollection.TYPE_DOCUMENT;
import static org.terifan.raccoon._Tools.createSecureStorage;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class BTreeNGTest
{
	@Test
	public void testOpenCloseSecureBTree()
	{
		Supplier<IPhysicalBlockDevice> device = () -> new MemoryBlockDevice(512);
//		Supplier<IPhysicalBlockDevice> device = () -> new FileBlockDevice(new File("d:/test.rdb"));

		ArrayMapKey key = new ArrayMapKey("key");
		byte[] value = "value".getBytes();

		try (BlockAccessor storage = createSecureStorage(device); BTree tree = new BTree(storage, new Document()))
		{
			tree.put(new ArrayMapEntry(key, value, TYPE_DOCUMENT));
			tree.commit();
			storage.getBlockDevice().getApplicationMetadata().put("conf", tree.getConfiguration());
			storage.getBlockDevice().commit();
		}

		try (BlockAccessor storage = createSecureStorage(device); BTree tree = new BTree(storage, storage.getBlockDevice().getApplicationMetadata().getDocument("conf")))
		{
			ArrayMapEntry entry = new ArrayMapEntry(key);
			assertTrue(tree.get(entry));
			assertEquals(entry.getValue(), value);
			storage.getBlockDevice().commit();
		}
	}
}
