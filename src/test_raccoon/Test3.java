package test_raccoon;

import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;


public class Test3
{
	public static void main(String... args)
	{
		try
		{
			MemoryBlockStorage blockDevice = new MemoryBlockStorage(512);
//			AccessCredentials ac = new AccessCredentials("password");
			AccessCredentials ac = null;

			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.REPLACE, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
			{
				for (String s : _WordLists.list26)
				{
					db.getCollection("words").saveOne(new Document().put("word", s));
				}

				db.commit();
//				showTree(db.getCollection("words").getImplementation());
			}

//			blockDevice.dump();

			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.OPEN, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				db.getCollection("words").saveOne(new Document().put("word", "test"));
				db.commit();
			}

			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.OPEN, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				db.getCollection("words").find().forEach(e -> System.out.println(e));

//				showTree(db.getCollection("words").getImplementation());
				db.commit();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
