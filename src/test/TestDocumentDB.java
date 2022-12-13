package test;

import java.util.List;
import org.terifan.bundle.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.LogLevel;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;
import org.terifan.raccoon.util.Log;
import static test._Tools.showTree;


// https://www.tutorialspoint.com/mongodb/mongodb_java.htm
public class TestDocumentDB
{
	public static void main(String... args)
	{
		try
		{
			Log.setLevel(LogLevel.DEBUG);

			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
			AccessCredentials ac = new AccessCredentials("password");
//			AccessCredentials ac = null;

			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.CREATE_NEW, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.CREATE_NEW))
			{
				for (int i = 0, z=0; i < 1; i++)
				{
					for (int j = 0; j < 2; j++,z++)
					{
						db.getCollection("people").save(new Document().putNumber("_id", z).putString("name", "olle-"+i+"-"+j));
					}
				}

//				RaccoonCollection collection = db.createCollection("people");
//				Document document = new Document("title", "MongoDB")
//					.putString("description", "database")
//					.putNumber("likes", 100)
//					.putString("url", "http://www.tutorialspoint.com/mongodb/")
//					.putString("by", "tutorials point");
//				collection.insert(document);
//
//				collection.find().forEach(doc -> System.out.println(doc));

				db.commit();

//				showTree(db.getCollection("people").getTableImplementation());
			}

//			blockDevice.dump();

			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.OPEN, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN))
			{
				Document doc = db.getCollection("people").get(new Document().putNumber("_id", 0));
				System.out.println(doc);

				List<Document> docs = db.getCollection("people").list(100);
				System.out.println(docs);

				db.getCollection("people").iterator().forEachRemaining(e->System.out.println(e));
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
