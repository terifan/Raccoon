package test_raccoon;

import java.nio.file.Paths;
import org.terifan.logging.Level;
import org.terifan.logging.Logger;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonHeap;


public class TestSimpleHashTable
{
	public static void main(String... args)
	{
		try
		{
			Logger.getLogger().setLevel(Level.OFF);

			// räkna ut hash
			// spara med _id som första 8 bitarna, finns det redan en post gå vidare
			// spara med _id som första 16 bitarna, finns det redan en post gå vidare
			// spara med _id som första 24 bitarna, finns det redan en post uppgradera den till en array om det inte redan är det och spara
			// för att ladda
			// testa med 8 bitar, om det record har rätt nyckel returnera
			// testa med 16 bitar, om det record har rätt nyckel returnera
			// testa med 24 bitar, om det record har rätt nyckel returnera, om det är en array, loopa igenom

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.REPLACE, null))
			{
				try (RaccoonHeap heap = db.getHeap("my_heap"))
				{
					for (int i = 0; i < 100; i++)
					{
						String key = "" + i;
						int _id = Math.abs(key.hashCode());
						System.out.println(_id);

						heap.put(_id, Document.of("hello:" + i));
					}
				}
				db.commit();
			}

			System.out.println("-".repeat(200));

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.READ_ONLY, null))
			{
				try (RaccoonHeap heap = db.getHeap("my_heap"))
				{
					for (int i = 0; i < 100; i++)
					{
						String key = "" + i;
						int _id = Math.abs(key.hashCode());
						System.out.println(_id);

						System.out.println(i + " = " + heap.get(_id));
					}
				}
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
