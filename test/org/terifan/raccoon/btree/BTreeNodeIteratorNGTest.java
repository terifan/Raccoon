package org.terifan.raccoon.btree;

import org.terifan.bundle.Document;
import static org.terifan.raccoon.RaccoonCollection.TYPE_DOCUMENT;
import org.testng.annotations.Test;
import static org.terifan.raccoon.btree._Tools.createStorage;
import static org.terifan.raccoon.btree._Tools.showTree;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;


public class BTreeNodeIteratorNGTest
{
	@Test
	public void test() throws InterruptedException
	{
		IPhysicalBlockDevice device = new MemoryBlockDevice(512);
//		Supplier<IPhysicalBlockDevice> device = () -> new FileBlockDevice(new File("d:/test.rdb"));

		try (BTreeStorage storage = createStorage(device); BTree tree = new BTree(storage, new Document()))
		{
			for (int i = 0; i < 100; i++)
			{
				tree.put(new ArrayMapEntry(("key"+i).getBytes(), ("value"+i).getBytes(), TYPE_DOCUMENT));
			}
			tree.commit();
			storage.getApplicationHeader().putBundle("conf", tree.getConfiguration());
		}

		try (BTreeStorage storage = createStorage(device); BTree tree = new BTree(storage, storage.getApplicationHeader().getBundle("conf")))
		{
			tree.iterator().forEachRemaining(e -> System.out.println(e));

			showTree(tree);

//			for (int i = 0; i < 100; i++)
//			{
//				ArrayMapEntry entry = new ArrayMapEntry(("key"+i).getBytes());
//				assertTrue(tree.get(entry));
//				assertEquals(entry.getValue(), ("value"+i).getBytes());
//			}
		}

		Thread.sleep(100000);
	}
}
