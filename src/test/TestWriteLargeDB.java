package test;

import java.io.File;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseOpenOption;
import test.TestTiny.Fruit;


/*
09:41:13    1000000       6943       5870       8778
09:41:29    2000000       6812       5475       9866
09:41:53    3000000       5844       6535      17959
09:42:16    4000000       6412       5470      16830
09:42:54    5000000       5244       7448      31121
09:43:37    6000000       3394       5535      37347
09:45:12    7000000       4449       4835      90273
09:46:27    8000000       3956       5859      69317
09:48:28    9000000       1286       9622     110950
09:50:43   10000000       3789       6120     128491
*/
public class TestWriteLargeDB
{
	public static void main(String... args)
	{
		try
		{
			Runtime r = Runtime.getRuntime();
			long startTime = System.currentTimeMillis();

			try ( Database db = new Database(new File("d:\\test.rdb"), DatabaseOpenOption.CREATE_NEW))
			{
				for (int j = 0, index = 0; j < 10; j++)
				{
					long t0 = System.currentTimeMillis();

					for (int i = 0; i < 1000_000; i++)
					{
						db.save(new Fruit(++index, "apple " + index, 1.4));
					}

					long t1 = System.currentTimeMillis();

					db.commit();

					long t2 = System.currentTimeMillis();

					System.out.printf("%s %10d %10d %10d %10d%n", System.currentTimeMillis() - startTime, index, (r.maxMemory() - r.totalMemory() + r.freeMemory()) / 1024 / 1024, t1 - t0, t2 - t1);
				}
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
