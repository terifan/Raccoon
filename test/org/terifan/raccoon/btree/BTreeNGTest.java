package org.terifan.raccoon.btree;

import org.terifan.bundle.Document;
import static org.terifan.raccoon.RaccoonCollection.TYPE_DOCUMENT;
import static org.terifan.raccoon.btree._Tools.createSecureStorage;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import static org.testng.Assert.*;
import org.testng.annotations.Test;


public class BTreeNGTest
{
	@Test
	public void testOpenCloseSecureBTree()
	{
		IPhysicalBlockDevice device = new MemoryBlockDevice(512);
//		Supplier<IPhysicalBlockDevice> device = () -> new FileBlockDevice(new File("d:/test.rdb"));

		byte[] key = "key".getBytes();
		byte[] value = "value".getBytes();

		try (BTreeStorage storage = createSecureStorage(device); BTree tree = new BTree(storage, new Document()))
		{
			tree.put(new ArrayMapEntry(key, value, TYPE_DOCUMENT));
			tree.commit();
			storage.getApplicationHeader().putBundle("conf", tree.getConfiguration());
		}

		try (BTreeStorage storage = createSecureStorage(device); BTree tree = new BTree(storage, storage.getApplicationHeader().getBundle("conf")))
		{
			ArrayMapEntry entry = new ArrayMapEntry(key);
			assertTrue(tree.get(entry));
			assertEquals(entry.getValue(), value);
		}
	}
}
