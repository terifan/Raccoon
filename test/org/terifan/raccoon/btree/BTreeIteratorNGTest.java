package org.terifan.raccoon.btree;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import static org.terifan.raccoon.RaccoonCollection.TYPE_DOCUMENT;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import org.terifan.raccoon.document.Document;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;


public class BTreeIteratorNGTest
{
	@Test
	public void testIterator() throws Exception
	{
		Map<Long, byte[]> data;

		Random rnd = new Random(1);
		HashSet<String> unique = new HashSet<>();
		while (unique.size() < 10000)
		{
			unique.add(String.format("key%09d", Math.abs(rnd.nextInt(1000000000))));
		}
		String[] keys = unique.toArray(String[]::new);

		try (MemoryBlockStorage device = new MemoryBlockStorage(512))
		{
			try (BlockAccessor storage = new BlockAccessor(new ManagedBlockDevice(device), false); BTree tree = new BTree(storage, BTree.createDefaultConfig(512)))
			{
				for (String key : keys)
				{
					tree.put(new ArrayMapEntry(new ArrayMapKey(key), Document.of("a:" + key), TYPE_DOCUMENT));
				}
				tree.commit();
				storage.getBlockDevice().getMetadata().put("conf", tree.getConfiguration());
				storage.getBlockDevice().commit();
			}
			data = device.getStorage();
		}

		try (MemoryBlockStorage device = new MemoryBlockStorage(512, data))
		{
			try (BlockAccessor storage = new BlockAccessor(new ManagedBlockDevice(device), false); BTree tree = new BTree(storage, storage.getBlockDevice().getMetadata().getDocument("conf")))
			{
				for (String key : keys)
				{
					ArrayMapEntry entry = new ArrayMapEntry(new ArrayMapKey(key));
					assertTrue(tree.get(entry));
					assertEquals(entry.getValue(), Document.of("a:" + key));
				}

				AtomicInteger cnt = new AtomicInteger();
				BTreeIterator it = new BTreeIterator(tree);
				it.forEachRemaining(c -> cnt.incrementAndGet());
				assertEquals(cnt.get(), 10000);

				cnt.set(0);
				it = new BTreeIterator(tree);
				while (it.hasNext())
				{
					Document k = it.next();
					if (k.getString("a").matches(".*1|.*3|.*5|.*7|.*9"))
					{
						it.remove();
						cnt.incrementAndGet();
					}
				}

				assertEquals(cnt.get(), 10000 - 4963);
				assertEquals(tree.size(), 4963);
			}
		}
	}
}
