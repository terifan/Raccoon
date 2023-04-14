package test;

import java.nio.file.Files;
import java.nio.file.Paths;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.RuntimeDiagnostics;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.document.ObjectId;
import org.terifan.raccoon.io.secure.AccessCredentials;


public class TestSimple
{
	public static void main(String... args)
	{
		try
		{
//			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//			AccessCredentials ac = new AccessCredentials("password");
			AccessCredentials ac = null;

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.REPLACE, ac))
			{
//				db.createIndex("lookup", "people", "name");

				db.getCollection("people").saveAll(new Document().put("name", "adam"),
					new Document().put("name", "eve"),
					new Document().put("name", "steve").put("_id", 7),
					new Document().put("name", "barbara"),
					new Document().put("name", "bob"),
					new Document().put("name", "walter")
				);

				db.getCollection("lookup").saveAll(new Document().put("_id", "adam").put("id", ObjectId.randomId()),
					new Document().put("_id", "eve").put("id", ObjectId.randomId()),
					new Document().put("_id", "steve").put("id", ObjectId.randomId()),
					new Document().put("_id", "barbara").put("id", ObjectId.randomId()),
					new Document().put("_id", "bob").put("id", ObjectId.randomId()),
					new Document().put("_id", "walter").put("id", ObjectId.randomId())
				);

				byte[] bytes = Files.readAllBytes(Paths.get("d:\\pictures\\babe.jpg"));
				db.getCollection("files").save(new Document().put("_id",1).put("content", bytes));

				db.commit();
			}

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.OPEN, ac))
			{
				db.getCollection("people").listAll().forEach(System.out::println);
				db.getCollection("lookup").listAll().forEach(System.out::println);

				System.out.println(db.getCollection("files").get(new Document().put("_id", 1)).getBinary("content").length);

				db.getCollection("people").listAll().forEach(System.out::println);

				db.commit();
			}

			RuntimeDiagnostics.print();

//			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.OPEN, ac))
//			{
//				db.getCollection("people").list().forEach(e -> System.out.println(e));
//			}

//			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.OPEN, ac))
//			{
//				db.getCollection("people").list().forEach(e -> System.out.println(e));
//			}

//			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.OPEN, ac))
//			{
//				db.getCollection("people").list().forEach(e -> System.out.println(e));
//			}

//			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.OPEN, ac))
//			{
//				db.getCollection("people").list().forEach(e -> System.out.println(e));
//			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
