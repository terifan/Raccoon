package test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.terifan.bundle.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.io.physical.MemoryBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;


public class TestSimple
{
	public static void main(String... args)
	{
		try
		{
			MemoryBlockDevice blockDevice = new MemoryBlockDevice(512);
//			AccessCredentials ac = new AccessCredentials("password");
			AccessCredentials ac = null;

			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.REPLACE, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.REPLACE, ac))
			{
//				db.createIndex("lookup", "people", "name");

				db.getCollection("people").saveAll(
					new Document().putString("name", "adam"),
					new Document().putString("name", "eve"),
					new Document().putString("name", "steve").putNumber("_id", 7),
					new Document().putString("name", "barbara"),
					new Document().putString("name", "bob"),
					new Document().putString("name", "walter")
				);

				db.getCollection("lookup").saveAll(
					new Document().putString("_id", "adam").putNumber("id", 1),
					new Document().putString("_id", "eve").putNumber("id", 2),
					new Document().putString("_id", "steve").putNumber("id", 7),
					new Document().putString("_id", "barbara").putNumber("id", 8),
					new Document().putString("_id", "bob").putNumber("id", 9),
					new Document().putString("_id", "walter").putNumber("id", 10)
				);

				byte[] bytes = Files.readAllBytes(Paths.get("d:\\pictures\\babe.jpg"));
				db.getCollection("files").save(new Document().putBinary("content", bytes));

				db.commit();
			}

			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.OPEN, ac))
			{
				db.getCollection("people").list().forEach(e -> System.out.println(e));
				db.getCollection("lookup").list().forEach(e -> System.out.println(e));

				System.out.println(db.getCollection("files").get(new Document().putNumber("_id", 1)).getBinary("content").length);

				db.commit();
			}

//			try (RaccoonDatabase db = new RaccoonDatabase(new File("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
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
