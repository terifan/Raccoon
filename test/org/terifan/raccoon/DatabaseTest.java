package org.terifan.raccoon;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.io.MemoryBlockDevice;
import org.terifan.raccoon.io.Streams;
import org.terifan.raccoon.util.Log;
import org.testng.annotations.Test;
import static org.testng.Assert.*;


public class DatabaseTest
{
	public DatabaseTest()
	{
		Log.LEVEL = 0;
	}


	@Test
	public void testSingleTableInsertTiny() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

//		AccessCredentials
//		Compression
//		NodeSize
//		LeafSize
//		Checksum

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			database.save(new _Fruit("red", "apple", 123));
			database.commit();
			assertNull(database.integrityCheck());
		}

//		for (byte[] b : device.getStorage().values())
//		{
//			Log.hexDump(b);
//		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			_Fruit apple = new _Fruit("red", "apple");
			assertTrue(database.get(apple));
			assertEquals("apple", apple.name);
			assertEquals(123, apple.value);
		}
	}


	@Test
	public void testSingleTableInsertTiny2() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			database.save(new _Fruit("red", "apple", 123));
			database.save(new _Fruit("green", "apple", 456));
			database.commit();
			assertNull(database.integrityCheck());
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			_Fruit redApple = new _Fruit("red", "apple");
			assertTrue(database.get(redApple));
			assertEquals("apple", redApple.name);
			assertEquals(123, redApple.value);

			_Fruit greenApple = new _Fruit("green", "apple");
			assertTrue(database.get(greenApple));
			assertEquals("apple", greenApple.name);
			assertEquals(456, greenApple.value);
		}
	}


	@Test
	public void testSingleTableMultiInsert() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			for (int i = 0; i < 10000; i++)
			{
				database.save(new _Fruit("red", "apple-" + i, i));
			}
			database.commit();
			assertNull(database.integrityCheck());
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			for (int i = 0; i < 10000; i++)
			{
				_Fruit apple = new _Fruit("red", "apple-"+i);
				assertTrue(database.get(apple));
				assertEquals("apple-"+i, apple.name);
				assertEquals(i, apple.value);
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
				database.save(new _Fruit("red", "apple-" + i, i));
				database.save(new _Animal("dog-" + i, i));
			}
			database.commit();
			assertNull(database.integrityCheck());
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			for (int i = 0; i < 10000; i++)
			{
				_Fruit apple = new _Fruit("red", "apple-"+i);
				assertTrue(database.get(apple));
				assertEquals("apple-"+i, apple.name);
				assertEquals(i, apple.value);

				_Animal animal = new _Animal("dog-"+i);
				assertTrue(database.get(animal));
				assertEquals("dog-"+i, animal.name);
				assertEquals(i, animal.number);
			}
		}
	}


	@Test
	public void testSingleBlobEntry() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);
		byte[] content = new byte[10*1024*1024];
		new Random().nextBytes(content);

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			database.save(new _Blob1("my blob", content));
			database.commit();
			assertNull(database.integrityCheck());
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			_Blob1 blob = new _Blob1("my blob");
			assertTrue(database.get(blob));
			assertEquals("my blob", blob.name);
			assertEquals(content, blob.content);
		}
	}


	@Test
	public void testSingleTableMultiConcurrentInsert() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			try (_FixedThreadExecutor executor = new _FixedThreadExecutor(1f))
			{
				AtomicInteger n = new AtomicInteger();
				for (int i = 0; i < 10000; i++)
				{
					executor.submit(()->
					{
						try
						{
							int j = n.incrementAndGet();
							database.save(new _Fruit("red", "apple-" + j, j));
						}
						catch (Throwable e)
						{
							e.printStackTrace(System.out);
						}
					});
				}
			}
			database.commit();
			assertNull(database.integrityCheck());
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			try (_FixedThreadExecutor executor = new _FixedThreadExecutor(1f))
			{
				AtomicInteger n = new AtomicInteger();
				for (int i = 0; i < 10000; i++)
				{
					executor.submit(()->
					{
						try
						{
							int j = n.incrementAndGet();
							_Fruit apple = new _Fruit("red", "apple-"+j);
							assertTrue(database.get(apple));
							assertEquals("apple-"+j, apple.name);
							assertEquals(j, apple.value);
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


//	@Test
//	public void testSingleBlobPushStream() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//		byte[] content = new byte[10*1024*1024];
//		new Random().nextBytes(content);
//
//		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
//		{
//			try (OutputStream out = database.write(new _Blob2("my blob")))
//			{
//				out.write(content);
//			}
//			database.commit();
//			assertNull(database.integrityCheck());
//		}
//
//		try (Database database = Database.open(device, OpenOption.OPEN))
//		{
//			try (InputStream in = database.read(new _Blob2("my blob")))
//			{
//				assertNotNull(in);
//				assertArrayEquals(content, Streams.fetch(in));
//			}
//		}
//	}


	@Test
	public void testSingleBlobWriteFromStream() throws Exception
	{
		MemoryBlockDevice device = new MemoryBlockDevice(512);
		byte[] content = new byte[10*1024*1024];
		new Random().nextBytes(content);

		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
		{
			database.save(new _Blob2("my blob"), new ByteArrayInputStream(content));
			database.commit();
			assertNull(database.integrityCheck());
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			try (InputStream in = database.read(new _Blob2("my blob")))
			{
				assertNotNull(in);
				assertEquals(content, Streams.fetch(in));
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
				database.save(new _DiscriminatedNumber(numberNames[i], i));
			}
			database.commit();

			assertNull(database.integrityCheck());
		}

		try (Database database = Database.open(device, OpenOption.OPEN))
		{
			for (int i = 0; i < numberNames.length; i++)
			{
				_DiscriminatedNumber entity = new _DiscriminatedNumber(i);
				assertTrue(database.get(entity));
				assertEquals(numberNames[i], entity.name);
				assertEquals(i, entity.number);
				assertEquals((i&1)==1, entity.odd);
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
			db.save(new _Fruit("red", "apple", 3467));
			db.commit();
		}

		try (Database db = Database.open(device, OpenOption.OPEN, accessCredentials))
		{
			_Fruit fruit = new _Fruit("red", "apple");
			assertTrue(db.get(fruit));
			assertEquals(fruit.value, 3467);
		}
	}


//	@Test
//	public void testIterators() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		String[] numberNames = {"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"};
//
//		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
//		{
//			for (int i = 0; i < numberNames.length; i++)
//			{
//				database.save(new _DiscriminatedNumber(numberNames[i], i));
//			}
//			database.commit();
//
//			assertNull(database.integrityCheck());
//
//			Log.out.println("---------");
//
//			_DiscriminatedNumber discriminator = new _DiscriminatedNumber();
//			discriminator.odd = true;
//
//			database.iterable(_DiscriminatedNumber.class, discriminator).forEach(e -> Log.out.println(e.number+" "+e.name+" "+e.odd));
//
//			Log.out.println("---------");
//
//			database.stream(_DiscriminatedNumber.class, discriminator).forEach(e -> Log.out.println(e.number+" "+e.name+" "+e.odd));
//
//			Log.out.println("----sorted-----");
//
//			database
//				.stream(_DiscriminatedNumber.class, discriminator)
//				.filter(e -> e.odd)
//				.sorted((o1, o2) -> Integer.compare(o1.number, o2.number))
//				.forEach(e -> Log.out.println(e.number+" "+e.name+" "+e.odd));
//
//			Log.out.println("-----x----");
//
//			for (_DiscriminatedNumber e : database.list(_DiscriminatedNumber.class, discriminator))
//			{
//				Log.out.println(e.number+" "+e.name+" "+e.odd);
//			}
//
//			Log.out.println("---------");
//
//			for (_DiscriminatedNumber e : database.iterable(_DiscriminatedNumber.class, discriminator))
//			{
//				Log.out.println(e.number+" "+e.name+" "+e.odd);
//			}
//		}
//	}


//	@Test
//	public void testFieldOrder() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		_FieldOrder1 f1 = new _FieldOrder1(123, 456, "abc", 3.14);
//		_FieldOrder2 f2 = new _FieldOrder2(123, 456);
//
//		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
//		{
//			database.save(f1);
//			database.commit();
//
//			assertNull(database.integrityCheck());
//		}
//
//		try (Database database = Database.open(device, OpenOption.OPEN))
//		{
//			assertTrue(database.get(f2));
//			assertEquals(f1.a, f2.a);
//			assertEquals(f1.b, f2.b);
//			assertEquals(f1.s, f2.s);
//			assertEquals(f1.d, f2.d, 0);
//		}
//	}


//	@Test
//	public void testExtraFields() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		_ExtraField1 f1_i = new _ExtraField1(112, 123, "abc", 3.14);
//		_ExtraField2 f2_i = new _ExtraField2(134, 145, "def", 1.59, 100);
//		_ExtraField3 f3_i = new _ExtraField3(156, 167, "ghi", 2.63, "jkl");
//		_ExtraField1 f1_o_1 = new _ExtraField1(112);
//		_ExtraField2 f1_o_2 = new _ExtraField2(112);
//		_ExtraField3 f1_o_3 = new _ExtraField3(112);
//		_ExtraField1 f2_o_1 = new _ExtraField1(134);
//		_ExtraField2 f2_o_2 = new _ExtraField2(134);
//		_ExtraField3 f2_o_3 = new _ExtraField3(134);
//		_ExtraField1 f3_o_1 = new _ExtraField1(156);
//		_ExtraField2 f3_o_2 = new _ExtraField2(156);
//		_ExtraField3 f3_o_3 = new _ExtraField3(156);
//
////		try (Database database = Database.open(device, Options.CREATE)) { }
////		try (Database database = Database.open(device, Options.CREATE_NEW)) { }
////		try (Database database = Database.open(device, Options.OPEN)) { }
////		try (Database database = Database.open(device, Options.READ_ONLY)) { }
//
//		try (Database database = Database.open(device, OpenOption.CREATE_NEW))
//		{
//			database.save(f1_i);
//			database.save(f2_i);
//			database.save(f3_i);
//			database.commit();
//
//			assertNull(database.integrityCheck());
//		}
//
//		try (Database database = Database.open(device, OpenOption.OPEN))
//		{
//			assertTrue(database.get(f1_o_1));
//			assertTrue(database.get(f1_o_2));
//			assertTrue(database.get(f1_o_3));
//			assertTrue(database.get(f2_o_1));
//			assertTrue(database.get(f2_o_2));
//			assertTrue(database.get(f2_o_3));
//			assertTrue(database.get(f3_o_1));
//			assertTrue(database.get(f3_o_2));
//			assertTrue(database.get(f3_o_3));
//
////			assertEquals(f1_i.b, f1_o.a);
////			assertEquals(f1_i.s, f1_o.s);
////			assertEquals(f1_i.d, f1_o.d, 0);
//		}
//	}
}
