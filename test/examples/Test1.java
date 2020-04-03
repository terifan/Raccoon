package examples;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.terifan.raccoon.Blob;
import org.terifan.raccoon.BlobOpenOption;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.LogLevel;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.util.Log;
import org.testng.annotations.Test;
import static org.testng.Assert.*;
import resources.__TestUtils;
import resources.entities._BlobKey1K;


public class Test1
{
	@Test(enabled = false)
	public void test() throws IOException
	{
		Log.setLevel(LogLevel.DEBUG);

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(4096);

		byte[] writeAll = new byte[10 * 1024 * 1024];
		new Random().nextBytes(writeAll);

		try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW, CompressionParam.NO_COMPRESSION))
		{
			_BlobKey1K key = new _BlobKey1K("test");

			try (Blob blob = db.openBlob(key, BlobOpenOption.CREATE))
			{
				blob.write(ByteBuffer.wrap(writeAll));
			}

			db.commit();
		}

		try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN, CompressionParam.NO_COMPRESSION))
		{
			_BlobKey1K key = new _BlobKey1K("test");

			byte[] readAll;

			try (Blob blob = db.openBlob(key, BlobOpenOption.READ))
			{
				readAll = new byte[(int)blob.size()];
				blob.read(ByteBuffer.wrap(readAll));
			}

			assertEquals(writeAll, readAll);
		}
	}
}
