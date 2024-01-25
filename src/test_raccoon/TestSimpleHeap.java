package test_raccoon;

import java.nio.file.Paths;
import org.terifan.logging.Level;
import org.terifan.logging.Logger;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonHeap;


public class TestSimpleHeap
{
	public static void main(String... args)
	{
		try
		{
			Logger.getLogger().setLevel(Level.OFF);

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.REPLACE, null))
			{
				try (RaccoonHeap heap = db.getHeap("my_heap", Document.of("leaf:4096,node:4096,compression:zle,degree:128,record:128")))
				{
					long t = System.currentTimeMillis();
					for (int i = 0; i < 10_000_000; i++)
					{
						heap.save(Document.of("hello:" + i));
					}
					System.out.println(System.currentTimeMillis()-t);
				}
				long t = System.currentTimeMillis();
				db.commit();
				System.out.println(System.currentTimeMillis()-t);
			}

			System.out.println("-".repeat(200));

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.READ_ONLY, null))
			{
				try (RaccoonHeap heap = db.getHeap("my_heap"))
				{
					long t = System.currentTimeMillis();
					for (int i = 0, sz = (int)heap.size(); i < sz; i++)
					{
						heap.get(i);
					}
					System.out.println(System.currentTimeMillis()-t);
				}
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
