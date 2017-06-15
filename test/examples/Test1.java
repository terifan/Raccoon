package examples;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.LogLevel;
import org.terifan.raccoon.OpenOption;
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
		Log.mLevel = LogLevel.DEBUG;

		MemoryBlockDevice blockDevice = new MemoryBlockDevice(4096);

		byte[] writeAll = new byte[10 * 1024 * 1024];
		new Random().nextBytes(writeAll);

		try (Database db = Database.open(blockDevice, OpenOption.CREATE_NEW, CompressionParam.NONE))
		{
			_BlobKey1K key = new _BlobKey1K("test");

			db.save(key, new ByteArrayInputStream(writeAll));

			db.commit();
		}

		try (Database db = Database.open(blockDevice, OpenOption.OPEN, CompressionParam.NONE))
		{
			_BlobKey1K key = new _BlobKey1K("test");

			byte[] readAll = __TestUtils.readAll(db.read(key));

			assertEquals(writeAll, readAll);
		}
	}
}
