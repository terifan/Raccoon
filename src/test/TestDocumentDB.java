package test;

import org.terifan.bundle.Document;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.monitoring.MonitorInstance;


// https://www.tutorialspoint.com/mongodb/mongodb_java.htm
public class TestDocumentDB
{
	public static void main(String... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);

			try (Database db = new Database(blockDevice, DatabaseOpenOption.CREATE_NEW))
			{
				db.save(new Document().putString("_id","1").putString("name", "olle"));

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
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
