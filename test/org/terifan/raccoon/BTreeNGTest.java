package org.terifan.raccoon;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.terifan.raccoon.document.Document;
import static org.terifan.raccoon.RaccoonCollection.TYPE_DOCUMENT;
import static org.terifan.raccoon._Tools.createSecureStorage;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.storage.FileBlockStorage;
import static org.testng.Assert.*;
import org.testng.annotations.Test;
import static resources.__TestUtils.doc;
import org.terifan.raccoon.blockdevice.storage.BlockStorage;


public class BTreeNGTest
{
	@Test
	public void testOpenCloseSecureBTree() throws IOException
	{
		Path path = Files.createTempFile("raccoon", "db");

		ArrayMapKey key = new ArrayMapKey("key");
		Document value = doc(5);

		try (BlockStorage device = new FileBlockStorage(path))
		{
			try (BlockAccessor storage = createSecureStorage(device); BTree tree = new BTree(storage, new Document()))
			{
				tree.put(new ArrayMapEntry(key, value, TYPE_DOCUMENT));
				tree.commit();
				System.out.println(tree.getConfiguration());
				storage.getBlockDevice().getMetadata().put("conf", tree.getConfiguration());
				storage.getBlockDevice().commit();
			}
		}

		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

		try (BlockStorage device = new FileBlockStorage(path))
		{
			try (BlockAccessor storage = createSecureStorage(device); BTree tree = new BTree(storage, storage.getBlockDevice().getMetadata().getDocument("conf")))
			{
				ArrayMapEntry entry = new ArrayMapEntry(key);
				assertTrue(tree.get(entry));
				assertEquals(entry.getValue(), value);
				storage.getBlockDevice().commit();
			}
		}

		Files.deleteIfExists(path);
	}
}
