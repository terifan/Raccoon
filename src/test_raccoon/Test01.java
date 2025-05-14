package test_raccoon;

import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonDatabaseProvider;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.blockdevice.RaccoonStorage;


public class Test01
{
	public static void main(String... args)
	{
		try
		{
			RaccoonDatabaseProvider provider = new RaccoonDatabaseProvider(new RaccoonStorage().withBlockSize(512).inMemory());

			try (RaccoonDatabase db = provider.get(DatabaseOpenOption.REPLACE))
			{
				RaccoonCollection test = db.getCollection("test");
				for (int i = 0; i < 10000; i++)
				{
					test.saveOne(Document.of("_id:'" + String.format("%6d",i) + "'"));
				}
				test.iterator().forEachRemaining(d -> System.out.println(d));
//				System.out.println(test.find().get().size());
//				test.printTree();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
