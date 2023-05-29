package test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;


public class TestSimple
{
	public static void main(String... args)
	{
		try
		{
			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.REPLACE, null))
			{
				db.createIndex("names", "people", "name");
				db.createIndex("ratings", "people", "ratings/*");

				db.getCollection("people").saveAll(
					Document.of("name:adam,ratings:[1,2]"),
					Document.of("name:eve,ratings:[3,4]"),
					Document.of("name:steve,ratings:[1,3],_id:7"),
					Document.of("name:walter,ratings:[2,4],_id:3219649164198494619"),
					Document.of("name:barbara,ratings:[2],_id:superuser"),
					Document.of("name:bob,ratings:[1,2,3,4]").put("_id", UUID.randomUUID())
				);

				byte[] bytes = Files.readAllBytes(Paths.get("d:\\pictures\\62zqkw9mqo8a1.jpg"));

				db.getCollection("files").save(new Document().put("_id", 1).put("content", bytes));

				db.commit();
			}

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.OPEN, null))
			{
				System.out.println(db.getCollectionNames());

				db.getCollection("people").saveAll(
					Document.of("name:gregor,ratings:[5]")
				);

				db.getCollection("people").deleteAll(
					Document.of("_id:7")
				);

				System.out.println("-".repeat(100));
				System.out.println("files");
				db.getCollection("files").forEach(System.out::println);
				System.out.println("-".repeat(100));
				System.out.println("people");
				db.getCollection("people").forEach(System.out::println);
				System.out.println("-".repeat(100));
				System.out.println("index:names");
				db.getCollection("index:names").forEach(System.out::println);
				System.out.println("-".repeat(100));
				System.out.println("index:ratings");
				db.getCollection("index:ratings").forEach(System.out::println);
				System.out.println("-".repeat(100));
				System.out.println("system:indices");
				db.getCollection("system:indices").forEach(System.out::println);

				db.getCollection("people").saveAll(
					Document.of("_id:superuser,name:sam")
				);

				System.out.println("-".repeat(100));
				System.out.println("people");
				db.getCollection("people").forEach(System.out::println);
				System.out.println("-".repeat(100));
				System.out.println("index:names");
				db.getCollection("index:names").forEach(System.out::println);
				System.out.println("-".repeat(100));
				System.out.println("index:ratings");
				db.getCollection("index:ratings").forEach(System.out::println);

				System.out.println(db.getCollection("files").get(new Document().put("_id", 1)).getBinary("content").length);

				db.commit();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
