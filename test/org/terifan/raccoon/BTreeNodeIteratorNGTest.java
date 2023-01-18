package org.terifan.raccoon;

import org.terifan.raccoon.ArrayMapEntry;
import org.terifan.raccoon.BTree;
import org.terifan.raccoon.BTreeNodeIterator;
import org.terifan.raccoon.BTreeStorage;
import org.terifan.raccoon.document.Document;
import static org.terifan.raccoon.RaccoonCollection.TYPE_DOCUMENT;
import static org.terifan.raccoon._Tools.createStorage;
import static org.terifan.raccoon._Tools.showTree;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;
import org.testng.annotations.Test;


public class BTreeNodeIteratorNGTest
{
	@Test
	public void test() throws InterruptedException
	{
		IPhysicalBlockDevice device = new MemoryBlockDevice(512);

		try (BlockAccessor storage = createStorage(()->device); BTree tree = new BTree(storage, new Document()))
		{
			for (int i = 0; i < 100_000; i++)
			{
				tree.put(new ArrayMapEntry(new ArrayMapKey("key" + i), ("value"+i).getBytes(), TYPE_DOCUMENT));
			}

			new BTreeNodeIterator(tree).forEachRemaining(node -> {});
//System.out.println(node)
//			showTree(tree);
//			Thread.sleep(100000);
		}
	}
}
