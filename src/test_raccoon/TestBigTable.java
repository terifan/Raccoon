package test_raccoon;

import java.util.HashSet;
import java.util.Random;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonBuilder;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.RuntimeDiagnostics;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.document.ObjectId;


public class TestBigTable
{
	public static void main(String... args)
	{
		try
		{
			Random rnd = new Random(1);
			Runtime r = Runtime.getRuntime();

			System.out.printf("%8s %8s %8s %8s %8s%n", "count", "insert", "commit", "total", "memory");

			int s1 = 1;
			int s2 = 20 / s1;

			// HDD  1m @  4k -- 4:24
			// HDD 10m @ 16k -- 66:25
			// HDD 10m @  4k -- 80:12, 80:26
			// SSD 10m @  4k -- 25:10

			HashSet<ObjectId> keys = new HashSet<>();

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
						Document person = _Person.createPerson(rnd, k);
						people.save(person);
						keys.add(person.getObjectId("_id"));
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

			System.out.println("-".repeat(100));

			System.out.println("insert time: " + t);

			t = System.currentTimeMillis();
			try (RaccoonDatabase db = new RaccoonBuilder().path("c:\\temp\\bigtable.rdb").get())
			{
				System.out.println("size: " + db.getCollection("people").size());
			}
			t = System.currentTimeMillis() - t;
			System.out.println("size time: " + t);

			HashSet<Integer> unique = new HashSet<>();
			t = System.currentTimeMillis();
			try (RaccoonDatabase db = new RaccoonBuilder().path("c:\\temp\\bigtable.rdb").get())
			{
				db.getCollection("people").forEach(doc -> {
					unique.add(doc.getInt("index"));
				});
			}
			t = System.currentTimeMillis() - t;
			System.out.println("read all: " + t);

			System.out.println("keys: " + keys.size());
			System.out.println("unique: " + unique.size());
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
