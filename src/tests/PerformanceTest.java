package tests;

import java.io.File;
import org.terifan.raccoon.CompressionParam;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.util.Log;


public class PerformanceTest
{
	public static void main(String ... args)
	{
		try
		{
			try (Database db = Database.open(new File("i:/sample.db"), OpenOption.CREATE_NEW, CompressionParam.BEST_SPEED))
			{
				for (int i = 0; i < 1000000; i++)
				{
					db.save(new _Fruit1K("apple_" + i, 152));
				}
				db.commit();
			}

			Log.out.println("done");
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
