package examples;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.testng.annotations.Test;
import resources.entities._BlobKey1K;


public class Test1
{
	@Test //(enabled = false)
	public void test() throws IOException
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(4096);

		try (Database db = Database.open(blockDevice, OpenOption.CREATE_NEW, CompressionParam.BEST_SPEED))
		{
			_BlobKey1K key = new _BlobKey1K("test");

			db.save(key, new ByteArrayInputStream(new byte[1024 * 1024]));

			db.commit();
		}
	}
}
