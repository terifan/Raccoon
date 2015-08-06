package org.terifan.raccoon;

import tests._BlobKey1K;
import tests.__FixedThreadExecutor;
import tests._Fruit2K;
import tests._KeyValue1K;
import tests._Number1K1D;
import tests._Animal1K;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.io.IManagedBlockDevice;
import org.terifan.raccoon.io.ManagedBlockDevice;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.io.Streams;
import org.terifan.raccoon.io.UnsupportedVersionException;
import org.terifan.raccoon.util.Log;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import tests._Fruit1K;
import static tests.__TestUtils.createBuffer;
import static tests.__TestUtils.t;


public class DatabaseNGTest
{
	@Test
	public void testSingleTableInsertTiny() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			database.save(new _Fruit1K("apple", 123.0));
			database.commit();
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			_Fruit1K apple = new _Fruit1K("apple");

			assertTrue(database.get(apple));
			assertEquals(apple._name, "apple");
			assertEquals(apple.calories, 123.0);
		}
	}


	@Test
	public void testSingleTableInsertTiny2() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			database.save(new _Fruit2K("red", "apple", 123));
			database.save(new _Fruit2K("green", "apple", 456));
			database.commit();
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			_Fruit2K redApple = new _Fruit2K("red", "apple");
			assertTrue(database.get(redApple));
			assertEquals("apple", redApple._name);
			assertEquals(123, redApple.value);

			_Fruit2K greenApple = new _Fruit2K("green", "apple");
			assertTrue(database.get(greenApple));
			assertEquals("apple", greenApple._name);
			assertEquals(456, greenApple.value);
		}
	}


	@Test(dataProvider = "itemSizes")
	public void testSingleTableMultiInsert(int aSize) throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			for (int i = 0; i < aSize; i++)
			{
				database.save(new _Fruit2K("red", "apple-" + i, i));
			}

			database.commit();
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			assertEquals(aSize, database.size(_Fruit2K.class));

			for (int i = 0; i < aSize; i++)
			{
				_Fruit2K apple = new _Fruit2K("red", "apple-"+i);
				assertTrue(database.get(apple));
				assertEquals(apple._name, "apple-"+i);
				assertEquals(apple.value, i);
			}
		}
	}


	@Test
	public void testMultiTableInsert() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			for (int i = 0; i < 10000; i++)
			{
				database.save(new _Fruit2K("red", "apple-" + i, i));
				database.save(new _Animal1K("dog-" + i, i));
			}
			database.commit();
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			for (int i = 0; i < 10000; i++)
			{
				_Fruit2K apple = new _Fruit2K("red", "apple-"+i);
				assertTrue(database.get(apple));
				assertEquals(apple._name, "apple-"+i);
				assertEquals(apple.value, i);

				_Animal1K animal = new _Animal1K("dog-"+i);
				assertTrue(database.get(animal));
				assertEquals(animal._name, "dog-"+i);
				assertEquals(animal.number, i);
			}
		}
	}


	@Test
	public void testSingleTableMultiColumnKey() throws Exception
	{
		ArrayList<_Fruit2K> list = new ArrayList<>();

		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			for (int i = 0; i < 10; i++)
			{
				_Fruit2K item = new _Fruit2K("red", "apple" + i, 123, t(1000));
				list.add(item);
				database.save(item);
			}
			database.commit();
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			for (_Fruit2K entry : list)
			{
				_Fruit2K item = new _Fruit2K(entry._color, entry._name);

				assertTrue(database.get(item));
				assertEquals(item.shape, entry.shape);
				assertEquals(item.taste, entry.taste);
				assertEquals(item.value, entry.value);
			}
		}

		assertTrue(true);
	}


	@Test
	public void testSingleTableMultiConcurrentInsert() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			try (__FixedThreadExecutor executor = new __FixedThreadExecutor(1f))
			{
				AtomicInteger n = new AtomicInteger();
				for (int i = 0; i < 10000; i++)
				{
					executor.submit(()->
					{
						try
						{
							int j = n.incrementAndGet();
							database.save(new _Fruit2K("red", "apple-" + j, j));
						}
						catch (Throwable e)
						{
							e.printStackTrace(System.out);
						}
					});
				}
			}
			database.commit();
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			try (__FixedThreadExecutor executor = new __FixedThreadExecutor(1f))
			{
				AtomicInteger n = new AtomicInteger();
				for (int i = 0; i < 10000; i++)
				{
					executor.submit(()->
					{
						try
						{
							int j = n.incrementAndGet();
							_Fruit2K apple = new _Fruit2K("red", "apple-"+j);
							assertTrue(database.get(apple));
							assertEquals(apple._name, "apple-"+j);
							assertEquals(apple.value, j);
						}
						catch (IOException e)
						{
							e.printStackTrace(System.out);
						}
					});
				}
			}
		}
	}


	@Test
	public void testDiscriminator() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		String[] numberNames = {"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"};

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			for (int i = 0; i < numberNames.length; i++)
			{
				database.save(new _Number1K1D(numberNames[i], i));
			}
			database.commit();
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			for (int i = 0; i < numberNames.length; i++)
			{
				_Number1K1D entity = new _Number1K1D(i);
				assertTrue(database.get(entity));
				assertEquals(entity.name, numberNames[i]);
				assertEquals(entity._number, i);
				assertEquals(entity._odd, (i&1)==1);
			}
		}
	}


	@Test
	public void testDiscriminatorList() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		String[] numberNames = {"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"};

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			for (int i = 0; i < numberNames.length; i++)
			{
				database.save(new _Number1K1D(numberNames[i], i));
			}
			database.commit();
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			_Number1K1D disc = new _Number1K1D();
			disc._odd = !true;

			for (_Number1K1D item : database.list(_Number1K1D.class, disc))
			{
				Log.out.println(item);
			}
		}
	}


	@Test
	public void testEncryptedAccess() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);
		AccessCredentials accessCredentials = new AccessCredentials("password");

		try (Database db = Database.open(device, OpenOption.CREATE_NEW, accessCredentials))
		{
			db.save(new _Fruit2K("red", "apple", 3467));
			db.commit();
		}

		try (Database db = Database.open(device, OpenOption.OPEN, accessCredentials))
		{
			_Fruit2K fruit = new _Fruit2K("red", "apple");
			assertTrue(db.get(fruit));
			assertEquals(fruit.value, 3467);
		}
	}


	@Test
	public void testSaveEntryAsBlob() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);
		byte[] content = new byte[10*1024*1024];
		new Random().nextBytes(content);

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			database.save(new _KeyValue1K("my blob", content));
			database.commit();
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			_KeyValue1K blob = new _KeyValue1K("my blob");
			assertTrue(database.get(blob));
			assertEquals(blob._name, "my blob");
			assertEquals(blob.content, content);
		}
	}


	@Test
	public void testSaveBlobFromStream() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);
		byte[] content = new byte[10*1024*1024];
		new Random().nextBytes(content);

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			database.save(new _BlobKey1K("my blob"), new ByteArrayInputStream(content));
			database.commit();
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			try (InputStream in = database.read(new _BlobKey1K("my blob")))
			{
				assertNotNull(in);
				assertEquals(Streams.fetch(in), content);
			}
		}
	}


	@Test(expectedExceptions = UnsupportedVersionException.class)
	public void testDatabaseVersionConflict() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (IManagedBlockDevice blockDevice = new ManagedBlockDevice(device))
		{
			blockDevice.setExtraData(new byte[100]);
			blockDevice.allocBlock(100);
			blockDevice.commit();
		}

		try (Database db = Database.open(device, OpenOption.OPEN)) // throws exception
		{
			fail();
		}
	}


	@Test
	public void testEraseBlob() throws IOException
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
		ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice);

		_KeyValue1K in = new _KeyValue1K("apple", createBuffer(0, 1000_000));
		_KeyValue1K out = new _KeyValue1K("apple");

		try (Database db = Database.open(managedBlockDevice, OpenOption.CREATE_NEW))
		{
			db.save(in);
			db.commit();
		}

		try (Database db = Database.open(managedBlockDevice, OpenOption.OPEN))
		{
			db.get(out);

			db.remove(out);
			db.commit();
		}

		assertEquals(in.content, out.content);
		assertEquals(managedBlockDevice.getUsedSpace(), 5);
		assertTrue(managedBlockDevice.getFreeSpace() > 1000_000 / 512);
	}


	@Test
	public void testReplaceBlob() throws IOException
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
		ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice);

		_KeyValue1K in = new _KeyValue1K("apple", createBuffer(0, 1000_000));

		try (Database db = Database.open(managedBlockDevice, OpenOption.CREATE_NEW))
		{
			db.save(in);
			db.commit();
		}

		in.content = null;

		try (Database db = Database.open(managedBlockDevice, OpenOption.OPEN))
		{
			db.save(in);
			db.commit();
		}

		assertEquals(managedBlockDevice.getUsedSpace(), 5);
		assertTrue(managedBlockDevice.getFreeSpace() > 1000_000 / 512);
	}


	@DataProvider(name="itemSizes")
	private Object[][] itemSizes()
	{
		return new Object[][]{{1},{10},{1000}};
	}
}
