package test_raccoon;

import java.nio.file.Paths;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonHeap;


public class TestHeapPerformance
{
	public static void main(String... args)
	{
		try
		{
			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("c:\\temp\\test.rdb"), DatabaseOpenOption.REPLACE, null))
			{
				// 23s, 1 250 084 KB
				// 48s, 1 265 216 KB
				long t = System.currentTimeMillis();
				try (RaccoonHeap heap = db.getHeap("heap"))
				{
					for (int i = 0; i < 10_000_000; i++)
					{
						heap.save(Document.of("hello:" + i));
					}
				}
				System.out.println(System.currentTimeMillis()-t);
				t = System.currentTimeMillis();
				db.commit();
				System.out.println(System.currentTimeMillis()-t);

//				// 1m 59s, 1 076 796 KB
//				long t = System.currentTimeMillis();
//				RaccoonCollection collection = db.getCollection("collection");
//				for (int i = 0; i < 10_000_000; i++)
//				{
//					collection.save(Document.of("hello:" + i));
//				}
//				System.out.println(System.currentTimeMillis()-t);
//				t = System.currentTimeMillis();
//				db.commit();
//				System.out.println(System.currentTimeMillis()-t);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
