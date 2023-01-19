package org.terifan.raccoon;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.terifan.raccoon.document.Document;
import static org.terifan.raccoon.RaccoonCollection.TYPE_DOCUMENT;
import org.testng.annotations.Test;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.storage.BlockAccessor;
import static org.terifan.raccoon._Tools.createStorage;


public class BTreeEntryIteratorNGTest
{
	@Test
	public void test() throws InterruptedException
	{
		IPhysicalBlockDevice device = new MemoryBlockDevice(512);
//		Supplier<IPhysicalBlockDevice> device = () -> new FileBlockDevice(new File("d:/test.rdb"));

		try (BlockAccessor storage = createStorage(()->device); BTree tree = new BTree(storage, new Document()))
		{
			List<Integer> arr = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
			int seed = new Random().nextInt();
			System.out.println("SEED=" + seed);
			Collections.shuffle(arr, new Random(seed));
			for (int i = 0; i < 1000; i++)
			{
				int _i = arr.get(i);
				tree.put(new ArrayMapEntry(new ArrayMapKey("key"+_i), ("value"+_i).getBytes(), TYPE_DOCUMENT));
//				tree.put(new ArrayMapEntry(("key"+_i).getBytes(), new Document().putNumber("_id", i).putString("name", "olle-"+i).marshal(), TYPE_DOCUMENT));
//				showTree(tree);
//				Thread.sleep(1);
			}
			tree.commit();
			storage.getBlockDevice().getApplicationMetadata().put("conf", tree.getConfiguration());
		}

		try (BlockAccessor storage = createStorage(()->device); BTree tree = new BTree(storage, storage.getBlockDevice().getApplicationMetadata().getDocument("conf")))
		{
//			new BTreeNodeIterator(tree).forEachRemaining(e -> System.out.println(e));

//			for (int i = 0; i < 100; i++)
//			{
//				ArrayMapEntry entry = new ArrayMapEntry(("key"+i).getBytes());
//				assertTrue(tree.get(entry));
//				assertEquals(entry.getValue(), ("value"+i).getBytes());
//			}
		}

//		Thread.sleep(100000);
	}
}
