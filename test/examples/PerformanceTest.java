package examples;

import resources.entities._Fruit1K;
import java.io.File;
import java.io.IOException;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.testng.annotations.Test;


public class PerformanceTest
{
	@Test(enabled = false)
	public void test() throws IOException
	{
		try (Database db = Database.open(new File("d:/sample.db"), OpenOption.CREATE_NEW, CompressionParam.BEST_SPEED))
		{
			for (int i = 0; i < 1000000; i++)
			{
				db.save(new _Fruit1K("apple_" + i, 152));
			}
			db.commit();
		}
	}
}
