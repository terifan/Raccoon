package org.terifan.raccoon;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import static org.testng.Assert.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import resources.__TestUtils;
import static resources.__TestUtils.createRandomBuffer;
import resources.entities._BlobEntity;
import resources.entities._BlobKey1K;
import resources.entities._KeyValue1K;


public class BlobNGTest
{
//	@Test
//	public void testSaveBlob() throws Exception
//	{
//		byte[] out = __TestUtils.createRandomBuffer(0, 10_000_000);
//
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (Database database = new Database(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			_BlobEntity ent = new _BlobEntity();
//			ent._id = 1;
//			ent.blob = new Blob();
//			database.save(ent);
//
//			try (LobByteChannel bos = ent.blob.open(LobOpenOption.WRITE))
//			{
//				bos.writeAllBytes(out);
//			}
//
//			database.commit();
//		}
//
//		byte[] in;
//
//		try (Database database = new Database(device, DatabaseOpenOption.OPEN))
//		{
//			_BlobEntity ent = database.get(new _BlobEntity(1));
//
//			try (LobByteChannel bos = ent.blob.open(LobOpenOption.READ))
//			{
//				in = bos.readAllBytes();
//			}
//		}
//
//		assertEquals(out, in);
//	}


//	@Test
//	public void testReplaceBlob() throws Exception
//	{
//		byte[] out1 = __TestUtils.createRandomBuffer(0, 1_000_000);
//		byte[] out2 = __TestUtils.createRandomBuffer(0, 1_000_000);
//
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (Database database = new Database(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			_BlobEntity ent1 = new _BlobEntity();
//			ent1._id = 1;
//			ent1.blob = new Blob().consume(out1);
//			database.save(ent1);
//
//			_BlobEntity ent2 = new _BlobEntity();
//			ent2._id = 1;
//			ent2.blob = new Blob().consume(out2);
//			database.save(ent2);
//
//			database.commit();
//		}
//
//		byte[] in;
//
//		try (Database database = new Database(device, DatabaseOpenOption.OPEN))
//		{
//			_BlobEntity ent = database.get(new _BlobEntity(1));
//
//			try (LobByteChannel bos = ent.blob.open(LobOpenOption.READ))
//			{
//				in = bos.readAllBytes();
//			}
//		}
//
//		assertEquals(out2, in);
//	}


//	@Test(expectedExceptions = UnmanagedBlobException.class)
//	public void testReplaceBlobError() throws Exception
//	{
//		byte[] out1 = __TestUtils.createRandomBuffer(0, 1_000_000);
//		byte[] out2 = __TestUtils.createRandomBuffer(0, 1_000_000);
//
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (Database database = new Database(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			_BlobEntity ent1 = new _BlobEntity();
//			ent1._id = 1;
//			ent1.blob = new Blob().consume(out1);
//			database.save(ent1);
//
//			_BlobEntity ent2 = new _BlobEntity();
//			ent2._id = 1;
//			ent2.blob = new Blob().consume(out2);
//			database.save(ent2);
//
//			ent1.blob.readAllBytes();
//		}
//	}


//	@Test
//	public void testConsumeBlob() throws Exception
//	{
//		byte[] out = __TestUtils.createRandomBuffer(0, 10_000_000);
//
//		MemoryBlockDevice device = new MemoryBlockDevice(512);
//
//		try (Database database = new Database(device, DatabaseOpenOption.CREATE_NEW))
//		{
//			_BlobEntity ent = new _BlobEntity();
//			ent._id = 1;
//			ent.blob = new Blob().consume(out);
//			database.save(ent);
//
//			database.commit();
//		}
//
//		byte[] in;
//
//		try (Database database = new Database(device, DatabaseOpenOption.OPEN))
//		{
//			_BlobEntity ent = database.get(new _BlobEntity(1));
//
//			try (LobByteChannel bos = ent.blob.open(LobOpenOption.READ))
//			{
//				in = bos.readAllBytes();
//			}
//		}
//
//		assertEquals(out, in);
//	}


//	@Test(expectedExceptions = UnmanagedBlobException.class)
//	public void testReadUncommitedBlob() throws Exception
//	{
//		new Blob().readAllBytes();
//	}
}
