package test_raccoon;

import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonDatabaseProvider;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.blockdevice.RaccoonStorage;


public class TestTiny
{
	public static void main(String ... args)
	{
		try
		{
			RaccoonDatabaseProvider provider = new RaccoonDatabaseProvider(new RaccoonStorage().inMemory());
			try (RaccoonDatabase db = provider.get(DatabaseOpenOption.REPLACE))
			{
				RaccoonCollection people = db.getCollection("people");
				Document doc = Document.of("name:bob,age:17");
				people.saveOne(doc);
				System.out.println(doc);
			}

			try (RaccoonDatabase db = provider.get())
			{
				RaccoonCollection people = db.getCollection("people");
				System.out.println(people.find().get());
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
