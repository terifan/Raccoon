package org.terifan.raccoon.btree;

import org.terifan.bundle.Document;
import static org.terifan.raccoon.RaccoonCollection.TYPE_DOCUMENT;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;
import static org.terifan.raccoon.btree._Tools.createStorage;
import static org.terifan.raccoon.btree._Tools.createMemoryStorage;
import static org.terifan.raccoon.btree._Tools.createSecureStorage;
import static org.terifan.raccoon.btree._Tools.createSecureMemoryStorage;


public class BTreeNodeIteratorNGTest
{
	@Test
	public void testSomeMethod()
	{
		MemoryBlockDevice memoryBlockDevice = new MemoryBlockDevice(512);

		byte[] key = "aaaaaaaaaaaaa".getBytes();
		byte[] value = "AAAAAAAAAAA".getBytes();

//		try (BTreeStorage storage = createSecureMemoryStorage(memoryBlockDevice))
		try (BTreeStorage storage = createStorage(memoryBlockDevice))
		{
			Document conf = new Document();
			try (BTree tree = new BTree(storage, conf))
			{
				tree.put(new ArrayMapEntry(key, value, TYPE_DOCUMENT));
				tree.commit();
				storage.getBlockAccessor().getBlockDevice().getApplicationHeader().putBundle("conf", conf);
			}
		}

//		try (BTreeStorage storage = createSecureMemoryStorage(memoryBlockDevice))
		try (BTreeStorage storage = createStorage(memoryBlockDevice))
		{
			Document conf = storage.getBlockAccessor().getBlockDevice().getApplicationHeader().getBundle("conf");
			System.out.println(conf);

			try (BTree tree = new BTree(storage, conf))
			{
				ArrayMapEntry entry = new ArrayMapEntry(key);
				assertTrue(tree.get(entry));
				assertEquals(entry.getValue(), value);
			}
		}
	}
}
