package examples;

import resources.entities._Fruit1K;
import java.io.IOException;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.testng.annotations.Test;


public class PerformanceTest
{
	/**
	 * approx 2 minutes runtime
	 */
	@Test(enabled = false)
	public void test() throws IOException
	{
		MemoryBlockDevice blockDevice = new MemoryBlockDevice(4096);

		try (Database db = Database.open(blockDevice, OpenOption.CREATE_NEW, CompressionParam.BEST_SPEED))
		{
			for (int i = 0; i < 1000000;)
			{
				long t = System.nanoTime();

				for (int j = 0; j < 10000; j++, i++)
				{
					db.save(new _Fruit1K("apple_" + j, 152));
				}

				System.out.println(i+"\t"+(System.nanoTime()-t)/1000000.0);
			}
			db.commit();
		}
	}
}
