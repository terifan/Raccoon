package org.terifan.raccoon;

import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;


public class RaccoonDatabaseNGTest
{
	@Test
	public void testSingleTableInsertTiny() throws Exception
	{
		ManagedBlockDevice device = new ManagedBlockDevice(new MemoryBlockStorage(512));

		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE))
		{
			database.getCollection("fruits").saveOne(new _Fruit("apple", 123.0));
			database.commit();
		}

		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
		{
			_Fruit apple = new _Fruit("apple");

			assertTrue(database.getCollection("fruits").tryFindOne(apple));
			assertEquals(apple.get("_id"), "apple");
			assertEquals(apple.get("calories"), 123.0);
		}
	}


//	@Test
//	public void testSingleTableInsertTiny2() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			database.save(new _Fruit2K("red", "apple", 123));
//			database.save(new _Fruit2K("green", "apple", 456));
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			_Fruit2K redApple = new _Fruit2K("red", "apple");
//			assertTrue(database.tryGet(redApple));
//			assertEquals(redApple._name, "apple");
//			assertEquals(redApple.value, 123);
//
//			_Fruit2K greenApple = new _Fruit2K("green", "apple");
//			assertTrue(database.tryGet(greenApple));
//			assertEquals(greenApple._name, "apple");
//			assertEquals(greenApple.value, 456);
//		}
//	}
//
//
//	@Test
//	public void testSingleTableInsertDuplicateEntity() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			database.save(new _Fruit1K("apple", 123));
//			database.commit();
//
//			database.save(new _Fruit1K("apple", 456));
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			assertEquals(database.size(_Fruit1K.class), 1);
//			assertEquals((int)database.get(new _Fruit1K("apple")).calories, 456);
//		}
//	}
//
//
//	@Test
//	public void testCreateNew() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			database.save(new _Fruit1K("apple"));
//			database.save(new _Fruit1K("banana"));
//			database.save(new _Fruit1K("carrot"));
//			database.commit();
//
//			assertEquals(database.list(_Fruit1K.class).size(), 3);
//			assertEquals(database.size(_Fruit1K.class), 3);
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			assertEquals(database.list(_Fruit1K.class).size(), 0);
//			assertEquals(database.size(_Fruit1K.class), 0);
//		}
//	}
//
//
//	@Test
//	public void testMultiTableInsert() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			for (int i = 0; i < 10000; i++)
//			{
//				database.save(new _Fruit2K("red", "apple-" + i, i));
//				database.save(new _Animal1K("dog-" + i, i));
//			}
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			for (int i = 0; i < 10000; i++)
//			{
//				_Fruit2K apple = new _Fruit2K("red", "apple-" + i);
//				assertTrue(database.tryGet(apple));
//				assertEquals(apple._name, "apple-" + i);
//				assertEquals(apple.value, i);
//
//				_Animal1K animal = new _Animal1K("dog-" + i);
//				assertTrue(database.tryGet(animal));
//				assertEquals(animal._name, "dog-" + i);
//				assertEquals(animal.number, i);
//			}
//		}
//	}
//
//
//	@Test
//	public void testSingleTableMultiColumnKey() throws Exception
//	{
//		ArrayList<_Fruit2K> list = new ArrayList<>();
//
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			for (int i = 0; i < 10; i++)
//			{
//				_Fruit2K item = new _Fruit2K("red", "apple" + i, 123, t(1000));
//				list.add(item);
//				database.save(item);
//			}
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			for (_Fruit2K entry : list)
//			{
//				_Fruit2K item = new _Fruit2K(entry._color, entry._name);
//
//				assertTrue(database.tryGet(item));
//				assertEquals(item.shape, entry.shape);
//				assertEquals(item.taste, entry.taste);
//				assertEquals(item.value, entry.value);
//			}
//		}
//
//		assertTrue(true);
//	}
//
//
//	@Test
//	public void testSingleTableMultiConcurrentInsert() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			try (__FixedThreadExecutor executor = new __FixedThreadExecutor(1f))
//			{
//				AtomicInteger n = new AtomicInteger();
//				for (int i = 0; i < 10000; i++)
//				{
//					executor.submit(() ->
//					{
//						try
//						{
//							int j = n.incrementAndGet();
//							database.save(new _Fruit2K("red", "apple-" + j, j));
//						}
//						catch (Throwable e)
//						{
//							throw new IllegalStateException(e);
//						}
//					});
//				}
//			}
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			try (__FixedThreadExecutor executor = new __FixedThreadExecutor(1f))
//			{
//				AtomicInteger n = new AtomicInteger();
//				for (int i = 0; i < 10000; i++)
//				{
//					executor.submit(() ->
//					{
//						int j = n.incrementAndGet();
//						_Fruit2K apple = new _Fruit2K("red", "apple-" + j);
//						assertTrue(database.tryGet(apple));
//						assertEquals(apple._name, "apple-" + j);
//						assertEquals(apple.value, j);
//					});
//				}
//			}
//		}
//	}
//
//
//	@Test
//	public void testDiscriminator() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		String[] numberNames =
//		{
//			"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"
//		};
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			for (int i = 0; i < numberNames.length; i++)
//			{
//				database.save(new _Number1K1D(numberNames[i], i));
//			}
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			for (int i = 0; i < numberNames.length; i++)
//			{
//				_Number1K1D entity = new _Number1K1D(i);
//				assertTrue(database.tryGet(entity));
//				assertEquals(entity.name, numberNames[i]);
//				assertEquals(entity._number, i);
//				assertEquals(entity._odd, (i & 1) == 1);
//			}
//		}
//	}
//
//
//	@Test
//	public void testDiscriminatorList() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		String[] numberNames =
//		{
//			"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"
//		};
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			for (int i = 0; i < numberNames.length; i++)
//			{
//				database.save(new _Number1K1DS(numberNames[i], i, i % 3));
//			}
//
//			for (int j = 0; j < 3; j++)
//			{
//				int cnt = 0;
//				for (_Number1K1DS item : database.list(_Number1K1DS.class, new _Number1K1DS(j)))
//				{
//					assertEquals(item._number % 3, j);
//					cnt++;
//				}
//				assertEquals(cnt, j == 2 ? 3 : 4);
//			}
//
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			for (int j = 0; j < 3; j++)
//			{
//				int cnt = 0;
//				for (_Number1K1DS item : database.list(_Number1K1DS.class, new _Number1K1DS(j)))
//				{
//					assertEquals(item._number % 3, j);
//					cnt++;
//				}
//				assertEquals(cnt, j == 2 ? 3 : 4);
//			}
//		}
//	}
//
//
//	@Test
//	public void testDiscriminatorListEntity() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			_Number1K1DS a = new _Number1K1DS("a", 1, 9);
//			_Number1K1DS b = new _Number1K1DS("a", 2, 9);
//			_Number1K1DS c = new _Number1K1DS("a", 3, 9);
//
//			database.save(a);
//			database.save(b);
//			database.save(c);
//
//			assertEquals(database.list(a).size(), 3);
//		}
//	}
//
//
//	@Test
//	public void testDiscriminatorList2() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		String[] numberNames =
//		{
//			"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"
//		};
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			for (int i = 0; i < numberNames.length; i++)
//			{
//				database.save(new _Number1K1D(numberNames[i], i));
//			}
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			_Number1K1D d = new _Number1K1D();
//			d._odd = true;
//
//			for (_Number1K1D item : database.list(_Number1K1D.class, new DiscriminatorType<>(d)))
//			{
//				assertTrue(item._odd);
//			}
//		}
//	}
//
//
//	@Test
//	public void testGetTableViaDiscriminator() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			database.save(new _Number1K1D("a", 1));
//			database.save(new _Number1K1D("b", 2));
//			database.save(new _Number1K1D("c", 3));
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			assertEquals(database.size(new _Number1K1D(true)), 2);
//			assertEquals(database.size(new _Number1K1D(false)), 1);
//		}
//	}
//
//
//	@Test
//	public void testEncryptedAccess() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//		AccessCredentials accessCredentials = new AccessCredentials("password").setIterationCount(1);
//
//		try (RaccoonDatabase db = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW, accessCredentials))
//		{
//			db.save(new _Fruit2K("red", "apple", 3467));
//			db.commit();
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(device, DatabaseOpenOption.OPEN, accessCredentials))
//		{
//			_Fruit2K fruit = new _Fruit2K("red", "apple");
//			assertTrue(db.tryGet(fruit));
//			assertEquals(fruit.value, 3467);
//		}
//	}
//
//
//	@Test
//	public void testDatabaseNotChangedWhenZeroItemsDeleted() throws IOException
//	{
//		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//		ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice);
//
//		_Animal1K item = new _Animal1K("banana");
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.CREATE_NEW))
//		{
//			assertFalse(db.remove(item));
//			assertFalse(db.commit());
//		}
//	}
//
//
//	@Test
//	public void testOpenClose() throws IOException
//	{
//		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//		ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice);
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.CREATE_NEW))
//		{
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.CREATE))
//		{
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.OPEN))
//		{
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.CREATE_NEW))
//		{
//			db.commit();
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.CREATE))
//		{
//			db.commit();
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.OPEN))
//		{
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.CREATE_NEW))
//		{
//			db.rollback();
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.CREATE))
//		{
//			db.rollback();
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.OPEN))
//		{
//		}
//	}
//
//
//	@Test(expectedExceptions = InvalidPasswordException.class)
//	public void testBadPassword() throws Exception
//	{
//		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.CREATE, new AccessCredentials("password").setIterationCount(1)))
//		{
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.CREATE, new AccessCredentials("bad-password").setIterationCount(1)))
//		{
//		}
//
//		fail();
//	}
//
//
//	@Test(expectedExceptions = IllegalArgumentException.class)
//	public void testUnsupportedDevice() throws Exception
//	{
//		IManagedBlockDevice blockDevice = new ManagedBlockDevice(new MemoryBlockDevice(512));
//
//		try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.CREATE, new AccessCredentials("password").setIterationCount(1)))
//		{
//		}
//
//		fail();
//	}
//
//
//	@Test(expectedExceptions = InvalidPasswordException.class)
//	public void testUnsupportedData() throws Exception
//	{
//		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//		blockDevice.writeBlock(0, new byte[512], 0, 512, new long[2]);
//
//		try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.CREATE, new AccessCredentials("password").setIterationCount(1)))
//		{
//		}
//
//		fail();
//	}
//
//
//	@Test(expectedExceptions = FileAlreadyOpenException.class)
//	public void testFailOpenLockedDatabase() throws Exception
//	{
//		AccessCredentials ac = new AccessCredentials("password").setIterationCount(1);
//
//		File file = File.createTempFile("raccoon", ".tmp");
//		file.deleteOnExit();
//
//		try (RaccoonDatabase db = new RaccoonDatabase(file, DatabaseOpenOption.CREATE_NEW, ac))
//		{
//			try (RaccoonDatabase unused = new RaccoonDatabase(file, DatabaseOpenOption.OPEN, ac))
//			{
//				fail("unreachable");
//			}
//
//			db.save(new _Fruit1K("banana", 152));
//			db.commit();
//		}
//	}
//
//
////	@Test(expectedExceptions = UnsupportedVersionException.class)
////	public void testUnsupportedVersion() throws Exception
////	{
////		String s = "44ef4c34b93ef0bc3da1af0e0357586f6c19552e91c879f0cdc3e123c02f6a696c7965e12141686a1ded4107c93349766a5fb8418f4e5c8190ecbb9e7887801a8438cbaba1dcd85746305c5161203a1a24dde7e782ee88c78c2d3047b776932c99f0c3dcc4486a13925ce0cdee68414be325fec9a8376aa65fbb411aa2351c2acd7d6a079df51199080c8810b7af560841d29bfb0676b4861f42a111c010b2c3d7422a84409867a96ad3d547a40d32ffae561b7ae3062fd0d5df50c67e11eb507fe7c97d46475f9ca864ee71c5c2beefef29f94448bda35efceedcf4ad3679d023679f544ea77ac16b818385cb7603bc993baa438756a64a06a1f392ff3c86bc5d22f04d66c010199543ac9deb7b60f43399af499ba73f7a28f40dbf883e21d4325b37e5c7b21d1024ca0021fc21fb13a0ce0126efe25a9cc01b13773cc6a6eb31297cc0d0fad04ddf8cfde8697a9cc13e541991cbddaedc7f63e1c31dcc58828a11bdd7b9812a74e3895eb600d7126270169aa4977a11276281a65a777d3d014018a332ce5f8d649cf911633e6c59a8eb905353bf4906592724a4e4b02daa2c2a90645aaa03a42205e669d674d10670e43e0660bc2e7c217e3adcae95b393ae836ce1103d7d67be2aaa0b628332366b5cf3efcc6603426a50acfe1b8750ee4c7ef508db88e288a1f5ddfed31f113b74ac9eac98aa112c4dce2941471a50b6be0b8bece943af9f32823ebe72b5d02cba9e28b589f9d727ad3154e2fabc0c9c82d8da537f550f1d6523436b8033635f4f2d59b3d8907e0fe5b58868fc6c4edf78845b69984b4ffc7a091f41390c2402d5581afe9a4be81dc612dd42cbc81bdf4b055ef27a6787dc89b2d7a79cf554aa9a64f05a5a26002d784692efa613d4848914344d9ad3ab67d3c9435b7b5db968a3c8700ccbf93a79c733b8c688f6d087c19e147a3859bfe04b8c97be1dca5997ff10bc5c920d54c37bccb5c9d7045c433d316671e0eccf416204039fc288652e5a9ffa1878d4c8fcfd77842cb173fca5e81a5c54042d60f308660b46f32e55897527771a4432686726dd9a13f06ec9635523fb8ef6e951abb963f331760b027b90c2beaf522cd969cc63c40436d63a5562c5deefe7ac85a407d150ec968d9043afe20e6e93d4a35f57177c6a54d9d68abbf6e0e16688d13f5d29b31bd22ee38e3caddfe18d6f91631510f80894a57797eaff2649e0a02e1494dff8eb0f89aa02441bfa45e17cb8f4bf2c62696b9dda8572f34c0bc0d823eb4964f0c8ddb1eca3648d25a0208f22ac8cea45138bba03a55d71aab4a391de3da704589e10f644cd3249db5216e7bd30ae4bd67be7bd93091c99d811fec6615cbb8ece46cfded4565e736246e6b021799f671e8df58647e71cc0d5b37769b74b51dd372457f6be2807896032452b9e7ec5e7420b7689f6e31b70ae97dd05e657a1c3ac69bc48a23e4ed9afae9665bb937e5d449a0a44f039882b97ea09e53f61aa4b988083ca2ada48ad4e555f95900a238a190179c81bea7204d2e3c0270044565f07dc2ebf207b595eae37ee0027199a4f46fb44f7ec0188ca9c421736b2298aa01064f183b2148417000ea61cbb644fa015016bb2a978170220052ee6f0e8928eb2d5adb4e3ffe057e3464c12dd763a9abedbae62b36e5fb18370248bfbc6510974e5b3ffb99caf6924fe76e361e447a0e5fcbcb1e664bc7c64ff9291d91f372fc8d985548f7f29a8112ae1e30700e83ff29147fcdc49600637b5dfde1ee7cf2302c27fd4bd382e629f46a77658f55f3dc8877d1c554ab093d463bd59a800fe70a854484d7d8ec9845432eac4fed717f5b7e61d198e08e58b4b7ff2b9c5fe7e1cfb2e094e1063d93246a74ae5beffd8ea261ea56b39f3f5422f7a62df48153505d7d34503c20dcec84387da504e3b9fc3ba0f792ab457ffae77f04557dbaa721d1d45ce01c20c7ed1ece0056013f865788da6607c2cba78ac202af8083b521cc7a546e341857ec41bee568b6f867fcd991382639a23564f76a6dccccd67347a831672bae84cdf8ab300d1472457b10c0d2df711de300cbb6d415173e95b550f21b06b6a9a93163f7c9d561cd869aa95530f4716c0f3c58c02cbdb3699d7e51b72cf182d6cb06d59345ec49e06b1c82dc15e6562dfef29a2";
////		byte[] buf = new byte[s.length() / 2];
////		for (int i = 0; i < buf.length; i++)
////		{
////			buf[i] = (byte)Integer.parseInt(s.substring(2*i, 2*i+2), 16);
////		}
////
////		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
////		blockDevice.writeBlock(0, buf, 0, buf.length, 0);
////
////		try (Database db = new Database(blockDevice, OpenOption.CREATE, new AccessCredentials("password").setIterationCount(100)))
////		{
////		}
////
////		fail();
////	}
//	@Test
//	public void testGetTables() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			database.save(new _Fruit2K("red", "apple", 123));
//			database.save(new _Animal1K("dog"));
//			database.save(new _Fruit1K("banana"));
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			List<Table> tableMetadatas = database.getTables();
//
//			assertEquals(tableMetadatas.size(), 3);
//
//			HashSet<String> set = new HashSet<>(Arrays.asList(_Fruit1K.class.getName(), _Fruit2K.class.getName(), _Animal1K.class.getName()));
//			assertTrue(set.contains(tableMetadatas.get(0).getEntityName()));
//			assertTrue(set.contains(tableMetadatas.get(1).getEntityName()));
//			assertTrue(set.contains(tableMetadatas.get(2).getEntityName()));
//		}
//	}
//
//
//	@Test
//	public void testGetDiscriminators() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			database.save(new _Fruit1K1D("red", "a", 1));
//			database.save(new _Fruit1K1D("green", "b", 2));
//			database.save(new _Fruit1K1D("yellow", "c", 3));
//			database.save(new _Fruit1K1D("red", "d", 4));
//			database.save(new _Fruit1K1D("red", "e", 5));
//			database.save(new _Fruit1K1D("yellow", "f", 6));
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			List<DiscriminatorType<_Fruit1K1D>> list = database.getDiscriminators(_Fruit1K1D.class);
//
//			assertEquals(list.size(), 3);
//		}
//	}
//
//
//	@Test
//	public void testSizeDiscriminator() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			database.save(new _Number1K1D("a", 1));
//			database.save(new _Number1K1D("b", 2));
//			database.save(new _Number1K1D("c", 3));
//			database.save(new _Number1K1D("d", 4));
//			database.save(new _Number1K1D("e", 5));
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			assertEquals(database.size(new DiscriminatorType(new _Number1K1D(true))), 3);
//			assertEquals(database.size(new DiscriminatorType(new _Number1K1D(false))), 2);
//		}
//	}
//
//
//	@Test
//	public void testGet() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			database.save(new _Animal1K("dog"));
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			database.get(new _Animal1K("dog"));
//		}
//	}
//
//
//	@Test(expectedExceptions = NoSuchEntityException.class)
//	public void testGetFail() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			database.save(new _Animal1K("dog"));
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			database.get(new _Animal1K("cat"));
//		}
//	}
//
//
//	@Test
//	public void testClear() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			for (int i = 0; i < 10000; i++)
//			{
//				database.save(new _Animal1K("dog_" + i));
//			}
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			// TODO: check space
////			assertEquals(database.getBlockDevice().getFreeSpace(), 5);
//
//			for (int i = 0; i < 10000; i++)
//			{
//				assertTrue(database.tryGet(new _Animal1K("dog_" + i)));
//			}
//
//			database.clear(_Animal1K.class);
//
//			for (int i = 0; i < 10000; i++)
//			{
//				assertFalse(database.tryGet(new _Animal1K("dog_" + i)));
//			}
//
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			assertEquals(database.size(_Animal1K.class), 0);
//
//			// TODO: check space
////			assertEquals(database.getBlockDevice().getUsedSpace(), 13);
//		}
//	}
//
//
//	@Test
//	public void testClearBlobs() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		byte[][] buf = new byte[10][];
//
//		for (int i = 0; i < 10; i++)
//		{
//			buf[i] = __TestUtils.createRandomBuffer(0, 1024 * 1024 * (i + 1) / 2);
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			for (int i = 0; i < 10; i++)
//			{
//				try (LobByteChannel blob = database.openLob(new _BlobKey1K("dog_" + i), LobOpenOption.CREATE))
//				{
//					blob.writeAllBytes(buf[i]);
//				}
//			}
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			// TODO: check space
////			assertEquals(database.getBlockDevice().getFreeSpace(), 5);
//
//			for (int i = 0; i < 10; i++)
//			{
//				try (LobByteChannel blob = database.openLob(new _BlobKey1K("dog_" + i), LobOpenOption.READ))
//				{
//					assertEquals(blob.readAllBytes(), buf[i]);
//				}
//			}
//
//			database.clear(_BlobKey1K.class);
//
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			assertEquals(database.size(_BlobKey1K.class), 0);
//
//			// TODO: check space
////			assertEquals(database.getBlockDevice().getUsedSpace(), 11);
//		}
//	}
//
//
//	@Test
//	public void testClearDiscriminator() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			for (int i = 0; i < 10000; i++)
//			{
//				database.save(new _Number1K1D(i));
//			}
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			for (int i = 0; i < 10000; i++)
//			{
//				assertTrue(database.tryGet(new _Number1K1D(i)));
//			}
//
//			database.clear(new _Number1K1D(false));
//
//			for (int i = 0; i < 10000; i++)
//			{
//				assertEquals(database.tryGet(new _Number1K1D(i)), (i & 1) == 1);
//			}
//
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			assertEquals(database.size(new DiscriminatorType(new _Number1K1D(false))), 0);
//			assertEquals(database.size(new DiscriminatorType(new _Number1K1D(true))), 5000);
//
//			// TODO:
////			database.getTables().stream().forEach(e->Log.out.println(e));
//		}
//	}
//
//
//	@Test
//	public void testRemove() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			for (int i = 0; i < 10000; i++)
//			{
//				database.save(new _Animal1K("dog_" + i));
//			}
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			for (int i = 0; i < 10000; i++)
//			{
//				assertTrue(database.tryGet(new _Animal1K("dog_" + i)));
//			}
//
//			assertFalse(database.remove(new _Animal1K("cat")));
//
//			for (int i = 0; i < 10000; i++)
//			{
//				assertTrue(database.remove(new _Animal1K("dog_" + i)));
//			}
//
//			database.commit();
//		}
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			assertEquals(database.size(_Animal1K.class), 0);
//		}
//	}
//
//
////	@Test
////	public void testStream() throws Exception
////	{
////		MemoryBlockDevice device = new MemoryBlockDevice(512);
////
////		TreeSet<String> keys = new TreeSet<>();
////
////		try (Database database = new Database(device, OpenOption.CREATE_NEW))
////		{
////			for (int i = 0; i < 10000; i++)
////			{
////				keys.add("dog_"+i);
////				database.save(new _Animal1K("dog_"+i));
////			}
////			database.commit();
////		}
////
////		try (Database database = new Database(device, OpenOption.OPEN))
////		{
////			TreeSet<String> found = new TreeSet<>();
////			database.stream(_Animal1K.class).forEach(e->found.add(e._name));
////
////			assertEquals(found, keys);
////		}
////	}
////
////
////	@Test(timeOut = 1000)
////	public void testStreamMissingTable() throws Exception
////	{
////		MemoryBlockDevice device = new MemoryBlockDevice(512);
////
////		try (Database database = new Database(device, OpenOption.CREATE_NEW))
////		{
////			database.stream(_Fruit1K.class).forEach(e->fail());
////		}
////	}
//	@Test(expectedExceptions = NoSuchEntityException.class)
//	public void testGetNonExistingTable() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			assertEquals(null, database.get(new _Fruit1K("test")));
//			database.commit();
//		}
//	}
//
//
//	@Test
//	public void testTryGetNonExistingTable() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			assertEquals(database.list(_Fruit1K.class).size(), 0);
//			assertEquals(database.size(_Fruit1K.class), 0);
//			assertFalse(database.tryGet(new _Fruit1K("test")));
//		}
//	}
//
//
//	@Test
//	public void testTableChange() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			database.save(new _Fruit1K("test"));
//			assertTrue(database.isModified());
//			database.rollback();
//			assertFalse(database.isModified());
//		}
//	}
//
//
//	@Test
//	public void testMultiThreadInsertUpdateDeleteAndRollbackOnUncommit() throws Exception
//	{
//		MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//
//		ManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(blockDevice);
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.CREATE))
//		{
//			try (__FixedThreadExecutor executor = new __FixedThreadExecutor(8))
//			{
//				for (int i = 0; i < 10000; i++)
//				{
//					int j = i;
//					executor.submit(() -> db.save(new _Fruit1K("apple_" + j)));
//					executor.submit(() -> db.save(new _Fruit1K("carrot_" + j)));
//					if ((i % 1000) == 0)
//					{
//						executor.submit(() -> db.commit());
//					}
//				}
//			}
//			db.commit();
//		}
//
//		Random r = new Random();
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.CREATE))
//		{
//			try (__FixedThreadExecutor executor = new __FixedThreadExecutor(8))
//			{
//				for (int i = 0; i < 10000; i++)
//				{
//					int j = i;
//					executor.submit(() -> db.save(new _Fruit1K("banana_" + j)));
//					executor.submit(() -> db.save(new _Fruit1K("apple_" + r.nextInt(10000))));
//					executor.submit(() -> db.get(new _Fruit1K("apple_" + j)));
//					executor.submit(() -> db.remove(new _Fruit1K("carrot_" + j)));
//					if ((i % 1000) == 0)
//					{
//						executor.submit(() -> db.commit());
//					}
//				}
//			}
//			db.commit();
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.OPEN))
//		{
//			try (__FixedThreadExecutor executor = new __FixedThreadExecutor(8))
//			{
//				for (int i = 0; i < 10000; i++)
//				{
//					int j = i;
//					executor.submit(() -> db.save(new _Fruit1K("cocoa_" + j)));
//					executor.submit(() -> db.get(new _Fruit1K("apple_" + r.nextInt(10000))));
//					executor.submit(() -> db.get(new _Fruit1K("banana_" + r.nextInt(10000))));
//					executor.submit(() -> db.remove(new _Fruit1K("apple_" + r.nextInt(10000))));
//				}
//			}
//			// changes will rollback on close because missing commit
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(managedBlockDevice, DatabaseOpenOption.OPEN))
//		{
//			List<String> items = db.list(_Fruit1K.class).stream().map(e -> e._name).collect(Collectors.toList());
//
//			assertEquals(items.size(), 20000);
//
//			for (int i = 0; i < 10000; i++)
//			{
//				assertTrue(db.tryGet(new _Fruit1K("apple_" + i)));
//				assertTrue(db.tryGet(new _Fruit1K("banana_" + i)));
//				assertFalse(db.tryGet(new _Fruit1K("cocoa_" + i)));
//				assertFalse(db.tryGet(new _Fruit1K("carrot_" + i)));
//			}
//		}
//	}
//
//
//	@Test(expectedExceptions = ReadOnlyDatabaseException.class)
//	public void testCommitToReadOnly() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase db = new RaccoonDatabase(device, DatabaseOpenOption.CREATE))
//		{
//			db.save(new _Fruit1K("apple"));
//			db.commit();
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(device, DatabaseOpenOption.READ_ONLY))
//		{
//			db.save(new _Fruit1K("banana"));
//			db.commit();
//		}
//	}
//
//
//	@Test(expectedExceptions = DatabaseIOException.class)
//	public void testWriteReadOnly() throws Exception
//	{
//		File file = new File(UUID.randomUUID().toString());
//		file.deleteOnExit();
//
//		try (RaccoonDatabase db = new RaccoonDatabase(file, DatabaseOpenOption.READ_ONLY))
//		{
//		}
//	}
//
//
//	@Test(expectedExceptions = FileAlreadyOpenException.class)
//	public void testWriteLockedFile() throws Exception
//	{
//		File file = new File(UUID.randomUUID().toString());
//		file.deleteOnExit();
//
//		try (RaccoonDatabase db = new RaccoonDatabase(file, DatabaseOpenOption.CREATE))
//		{
//			try (RaccoonDatabase db2 = new RaccoonDatabase(file, DatabaseOpenOption.CREATE))
//			{
//			}
//		}
//	}
//
//
//	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*Block device is empty.*")
//	public void testReadEmptyFile() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase db = new RaccoonDatabase(device, DatabaseOpenOption.READ_ONLY))
//		{
//		}
//	}
//
//
//	@Test(expectedExceptions = UnsupportedVersionException.class, expectedExceptionsMessageRegExp = ".*Device tenant header labels don't match.*")
//	public void testDeviceLabel1() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase db = new RaccoonDatabase(device, DatabaseOpenOption.CREATE, new DeviceHeader("test")))
//		{
//			db.commit();
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(device, DatabaseOpenOption.READ_ONLY, new DeviceHeader("test2")))
//		{
//		}
//	}
//
//
//	@Test
//	public void testScan() throws IOException
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase db = new RaccoonDatabase(device, DatabaseOpenOption.CREATE))
//		{
//			for (int i = 0; i < 10_000; i++)
//			{
//				db.save(new _Fruit1K("fruits-" + i));
//			}
//			for (int i = 0; i < 10_000; i++)
//			{
//				db.save(new _Animal1K("animals-" + i));
//			}
//			for (int i = 0; i < 10_000; i++)
//			{
//				db.save(new _Number1K2D(i % 5, i % 7, i, "numbers-" + i));
//			}
//			db.commit();
//		}
//
//		try (RaccoonDatabase db = new RaccoonDatabase(device, DatabaseOpenOption.OPEN))
//		{
//			ScanResult scan = db.scan(null);
//
//			System.out.println(scan);
//
//			assertEquals(scan.tables, 38);
//			assertEquals(scan.records, 1 + 1 + 5 * 7 + 10000 + 10000 + 10000);
//		}
//	}
//
//
//	@Test
//	public void testGrowTreeRollback() throws Exception
//	{
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (RaccoonDatabase database = new RaccoonDatabase(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			for (int i = 0; i < 10; i++)
//			{
//				database.save(new _Fruit1K("a-" + i));
//			}
//
////			System.out.println(database.getTable(_Fruit1K.class).scan(null));
//
//			database.commit();
//
//			for (int i = 0; i < 10; i++)
//			{
//				assertTrue(database.tryGet(new _Fruit1K("a-" + i)));
//			}
//
//			for (int i = 0; i < 100; i++)
//			{
//				database.save(new _Fruit1K("b-" + i));
//			}
//
//			for (int i = 0; i < 100; i++)
//			{
//				assertTrue(database.tryGet(new _Fruit1K("b-" + i)));
//			}
//
//			database.rollback();
//
//			for (int i = 0; i < 10; i++)
//			{
//				assertTrue(database.tryGet(new _Fruit1K("a-" + i)));
//			}
//
//			for (int i = 0; i < 100; i++)
//			{
//				assertFalse(database.tryGet(new _Fruit1K("b-" + i)));
//			}
//		}
//	}
}
