package test_raccoon;

import java.util.List;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.document.Document;



public class Test4
{
	public static void xxmain(String... args)
	{
		try
		{
			try (RaccoonDatabase db = new RaccoonDatabase(new MemoryBlockStorage(512), DatabaseOpenOption.CREATE, null))
			{
				for (int i = 0; i < 30; i++)
				{
					for (int j = 0; j < 5; j++)
					{
						db.getCollection("data").save(new Document().put("_id", Array.of(i, j)).put("text", i + "," + j + ":xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"));
					}
				}

				// in
				// exists
				// lt
				// lte
				// gt
				// gte
				// ne
				// nin +array
				// or +array
				// regex

//				_Tools.showTree(db.getCollection("data")._getImplementation());

				Document x = db.getCollection("data").get(new Document().put("_id", Array.of(20, 2)));
				System.out.println(x);

				List<Document> list = db.getCollection("data").find(Document.of("_id:[{$gte:20,$lt:30},{$exists:true}]"));

				list.forEach(e -> System.out.print(e.get("_id") + "\t"));
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
