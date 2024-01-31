package test_raccoon;

import java.util.HashSet;
import java.util.Random;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonBuilder;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.RuntimeDiagnostics;
import org.terifan.raccoon.document.Document;


public class TestBigTable
{
	public static void main(String... args)
	{
		try
		{
			Random rnd = new Random(1);
			Runtime r = Runtime.getRuntime();

			System.out.printf("%8s %8s %8s %8s %8s%n", "count", "insert", "commit", "total", "memory");

			int s1 = 100;
			int s2 = 10_000_000 / s1;

			// HDD  1m @  4k -- 4:24
			// HDD 10m @ 16k -- 66:25
			// HDD 10m @  4k -- 80:12, 80:26
			// SSD 10m @  4k -- 25:10

			long t = System.currentTimeMillis();
			try (RaccoonDatabase db = new RaccoonBuilder().path("c:\\temp\\bigtable.rdb").get(DatabaseOpenOption.REPLACE))
			{
				RaccoonCollection people = db.getCollection("people");
				long t0 = System.currentTimeMillis();
				for (int j = 0, k = 0; j < s1; j++)
				{
					long t1 = System.currentTimeMillis();
					for (int i = 0; i < s2; i++, k++)
					{
						people.save(_Person.createPerson(rnd, k));
//						people.save(Document.of("index:" + k));
					}
					long t2 = System.currentTimeMillis();
					db.commit();
					long t3 = System.currentTimeMillis();
					System.gc();
					System.out.printf("%8d %8.1f %8.1f %8.1f %8d %s%n", s2 * (1 + j), (t2 - t1) / 1000f, (t3 - t2) / 1000f, (t3 - t0) / 1000f, (r.totalMemory() - r.freeMemory()) / 1024 / 1024, RuntimeDiagnostics.string());
					RuntimeDiagnostics.reset();
				}
			}
			t = System.currentTimeMillis() - t;
			System.out.println(t);

			System.out.println("-".repeat(100));

			t = System.currentTimeMillis();
			try (RaccoonDatabase db = new RaccoonBuilder().path("c:\\temp\\bigtable.rdb").get())
			{
				System.out.println(db.getCollection("people").size());
			}
			t = System.currentTimeMillis() - t;
			System.out.println(t);

			HashSet<Integer> unique = new HashSet<>();
			t = System.currentTimeMillis();
			try (RaccoonDatabase db = new RaccoonBuilder().path("c:\\temp\\bigtable.rdb").get())
			{
				db.getCollection("people").forEach(doc -> {
					unique.add(doc.getInt("index"));
				});
			}
			t = System.currentTimeMillis() - t;
			System.out.println(t);

			System.out.println(unique.size());
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
