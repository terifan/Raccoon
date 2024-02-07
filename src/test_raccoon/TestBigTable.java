package test_raccoon;

import java.util.HashSet;
import java.util.Random;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonBuilder;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.RuntimeDiagnostics;
import org.terifan.raccoon.ScanResult;
import static org.terifan.raccoon.blockdevice.util.ValueFormatter.formatBytesSize;
import static org.terifan.raccoon.blockdevice.util.ValueFormatter.formatDuration;
import static org.terifan.raccoon.blockdevice.util.ValueFormatter.formatCount;
import org.terifan.raccoon.document.Document;


public class TestBigTable
{
	public static void main(String ... args)
	{
		try
		{
			create(1_000_000, !false);
			measureSize();
			measureStats();
			loadAll();
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	private static void create(int aSize, boolean aPerson)
	{
		Random rnd = new Random(1);
		Runtime r = Runtime.getRuntime();

		System.out.printf("%12s %10s %10s %10s %9s%n", "count", "insert", "commit", "total", "memory");

		long t = System.currentTimeMillis();
		try (RaccoonDatabase db = new RaccoonBuilder().device("c:\\temp\\bigtable.rdb").get(DatabaseOpenOption.REPLACE))
		{
			long t0 = System.currentTimeMillis();
			long t1 = t0;
			RaccoonCollection collection = db.getCollection("people");
			for (int index = 0, transIndex = 0; index < aSize; index++, transIndex++)
			{
				if (aPerson)
				{
					collection.saveOne(_Person.createPerson(rnd, index));
				}
				else
				{
					collection.saveOne(Document.of("index:" + index));
				}

				if (index == aSize - 1 || transIndex == 100_000)
				{
//					System.out.printf("%12s %10s %10s %10s %9s -- %s%n", formatCount(index), "", "", "", formatBytesSize(r.totalMemory() - r.freeMemory()), RuntimeDiagnostics.string());

					long t2 = System.currentTimeMillis();
					db.commit();
					long t3 = System.currentTimeMillis();

					System.out.printf("%12s %10s %10s %10s %9s -- %s%n", formatCount(index), formatDuration(t2 - t1), formatDuration(t3 - t2), formatDuration(t3 - t0), formatBytesSize(r.totalMemory() - r.freeMemory()), RuntimeDiagnostics.string());
					RuntimeDiagnostics.reset();
					transIndex = 0;

					t1 = System.currentTimeMillis();
				}
			}
		}
		t = System.currentTimeMillis() - t;

		System.out.println("-".repeat(200));
		System.out.println("Insert time: " + formatDuration(t));
	}


	private static void measureStats()
	{
		try (RaccoonDatabase db = new RaccoonBuilder().device("c:\\temp\\bigtable.rdb").get())
		{
			ScanResult stats = db.getCollection("people")._getStats();

			System.out.println("stats: " + stats);

//				VerticalImageFrame frame = new VerticalImageFrame();
//				frame.add(new TreeGraph(new VerticalLayout(), stats.log.toString()));
		}
	}


	private static void measureSize()
	{
		long t = System.currentTimeMillis();
		try (RaccoonDatabase db = new RaccoonBuilder().device("c:\\temp\\bigtable.rdb").get())
		{
			System.out.println("size: " + formatCount(db.getCollection("people").size()));
		}
		t = System.currentTimeMillis() - t;
		System.out.println("size time: " + formatDuration(t));
	}


	private static void loadAll()
	{
		HashSet<Integer> unique = new HashSet<>();
		long t = System.currentTimeMillis();
		try (RaccoonDatabase db = new RaccoonBuilder().device("c:\\temp\\bigtable.rdb").get())
		{
			db.getCollection("people").forEach(doc -> {
				unique.add(doc.getInt("index"));
			});
		}
		t = System.currentTimeMillis() - t;
		System.out.println("read all time: " + formatDuration(t));

		System.out.println("total documents: " + formatCount(unique.size()));
	}
}
