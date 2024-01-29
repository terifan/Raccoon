package test_raccoon;

import java.util.Random;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonBuilder;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.RuntimeDiagnostics;


public class TestBigTable
{
	public static void main(String... args)
	{
		try
		{
			Random rnd = new Random();
			Runtime r = Runtime.getRuntime();

			System.out.printf("%8s %8s %8s %8s %8s%n", "count", "insert", "commit", "total", "memory");

			int s1 = 100;
			int s2 = 10_000_000 / s1;

			//  1m 4:24
			// 10m 

//			try (RaccoonDatabase db = new RaccoonBuilder().path("d:\\test.rdb").get(DatabaseOpenOption.REPLACE))
//			{
//				RaccoonCollection people = db.getCollection("people");
//				long t0 = System.currentTimeMillis();
//				for (int j = 0; j < s1; j++)
//				{
//					long t1 = System.currentTimeMillis();
//					for (int i = 0; i < s2; i++)
//					{
//						people.save(_Person.createPerson(rnd));
//					}
//					long t2 = System.currentTimeMillis();
//					db.commit();
//					long t3 = System.currentTimeMillis();
//					System.gc();
//					System.out.printf("%8d %8.1f %8.1f %8.1f %8d %s%n", s2 * (1 + j), (t2 - t1)/1000f, (t3 - t2)/1000f, (t3 - t0)/1000f, (r.totalMemory() - r.freeMemory()) / 1024 / 1024, RuntimeDiagnostics.string());
//					RuntimeDiagnostics.reset();
//				}
//			}

			System.out.println("-".repeat(100));

			try (RaccoonDatabase db = new RaccoonBuilder().path("d:\\test.rdb").get())
			{
				System.out.println(db.getCollection("people").size());
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
