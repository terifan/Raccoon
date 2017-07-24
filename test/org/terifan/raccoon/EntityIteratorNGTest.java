package org.terifan.raccoon;

import java.io.IOException;
import java.util.List;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import resources.entities._Fruit1K;
import resources.entities._Fruit1KInit;


public class EntityIteratorNGTest
{
	public EntityIteratorNGTest()
	{
	}


	@Test
	public void testInitializer() throws IOException
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		try (Database db = new Database(blockDevice, OpenOption.CREATE))
		{
			db.save(new _Fruit1K("apple", 52.12));
			db.save(new _Fruit1K("banana", 42.12));
			db.save(new _Fruit1K("orange", 32.12));
			db.commit();
		}

		try (Database db = new Database(blockDevice, OpenOption.OPEN))
		{
			db.setInitializer(_Fruit1K.class, e->{e.calories=-e.calories;});

			List<_Fruit1K> list = db.list(_Fruit1K.class);

			assertEquals(list.size(), 3);

			for (_Fruit1K item : list)
			{
				assertTrue(item.calories < 0);
			}
		}
	}


	@Test
	public void testInitializable() throws IOException
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

		try (Database db = new Database(blockDevice, OpenOption.CREATE))
		{
			db.save(new _Fruit1KInit("apple", 52.12));
			db.save(new _Fruit1KInit("banana", 42.12));
			db.save(new _Fruit1KInit("orange", 32.12));
			db.commit();
		}

		try (Database db = new Database(blockDevice, OpenOption.OPEN))
		{
			List<_Fruit1KInit> list = db.list(_Fruit1KInit.class);

			assertEquals(list.size(), 3);

			for (_Fruit1KInit item : list)
			{
				assertTrue(item.initialized);
			}
		}
	}
}
