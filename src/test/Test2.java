package test;

import java.io.File;
import java.util.List;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.LogLevel;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;
import org.terifan.raccoon.util.Log;
import static test._Tools.showTree;


// https://www.tutorialspoint.com/mongodb/mongodb_java.htm
public class Test2
{
	public static void main(String... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//			AccessCredentials ac = new AccessCredentials("password");
			AccessCredentials ac = null;

			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.REPLACE, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
			{
				RaccoonCollection collection = db.getCollection("words");
				for (String word : _WordLists.list130)
				{
					collection.save(new Document().put("_id", word));
				}

				db.commit();

//				showTree(db.getCollection("words"));
			}

			blockDevice.dump();

			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.OPEN, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
			{
				db.getCollection("words").list().forEach(e -> System.out.println(e));
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
