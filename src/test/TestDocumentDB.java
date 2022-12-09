package test;

import java.io.File;
import java.util.List;
import org.terifan.bundle.Document;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseOpenOption;


// https://www.tutorialspoint.com/mongodb/mongodb_java.htm
public class TestDocumentDB
{
	public static void main(String... args)
	{
		try
		{
//			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

//			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW))
			try (Database db = new Database(new File("d:\\test.rdb"), DatabaseOpenOption.CREATE_NEW))
			{
				for (int i = 0, z=0; i < 100; i++)
				{
					for (int j = 0; j < 100; j++,z++)
					{
						db.getCollection("people").save(new Document().putNumber("_id", z).putString("name", "olle-"+i+"-"+j));
					}
					db.commit();
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
			}

//			blockDevice.dump();

//			try (Database db = new Database(blockDevice, DatabaseOpenOption.OPEN))
			try (Database db = new Database(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN))
			{
				Document doc = db.getCollection("people").get(new Document().putNumber("_id", 0));
				System.out.println(doc);

				List<Document> docs = db.getCollection("people").list(100);
				System.out.println(docs);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
