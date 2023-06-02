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
				db.createIndex("peopleFirstName", "people", false, "firstName");
				db.createIndex("peopleLastName", "people", false, "lastName");

				db.getCollection("people").saveAll(
					Document.of("firstName:adam,lastName:irwing,language:[en,fr],ratings:[1,2]"),
					Document.of("firstName:eve,lastName:king,language:[en],ratings:[1,3]"),
					Document.of("firstName:steve,lastName:king,language:[kr],ratings:[1,2,3,4],_id:7"),
					Document.of("firstName:walter,lastName:black,language:[en],ratings:[3,4],_id:3219649164198494619"),
					Document.of("firstName:barbara,lastName:black,language:[en,fr,de],ratings:[3],_id:superuser"),
					Document.of("firstName:bob,lastName:townhill,language:[en,de]").put("_id", UUID.randomUUID())
				);

				byte[] bytes = Files.readAllBytes(Paths.get("d:\\pictures\\62zqkw9mqo8a1.jpg"));

				db.getCollection("files").save(new Document().put("_id", 1).put("content", bytes));

				db.commit();
			}

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.OPEN, null))
			{
				System.out.println(db.getCollectionNames());

				db.getCollection("people").saveAll(
					Document.of("firstName:gregor,lastName:king,language:[en,ru],ratings:[1]")
				);

				db.getCollection("people").deleteAll(
					Document.of("_id:7")
				);

				System.out.println(db.getCollectionNames());

				db.getCollection("files").listAll().forEach(System.out::println);
				db.getCollection("people").listAll().forEach(System.out::println);
				db.getCollection("index:peopleFirstName").listAll().forEach(System.out::println);
				db.getCollection("index:peopleLastName").listAll().forEach(System.out::println);
				db.getCollection("system:indices").listAll().forEach(System.out::println);

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
					Document.of("_id:superuser,firstName:anne,lastName:black,ratings:[1,2,4]")
				);

				db.getCollection("people").listAll().forEach(System.out::println);
				db.getCollection("index:peopleFirstName").listAll().forEach(System.out::println);
				db.getCollection("index:peopleLastName").listAll().forEach(System.out::println);

				System.out.println("-".repeat(100));
				System.out.println(db.getCollection("files").get(new Document().put("_id", 1)).getBinary("content").length);

				System.out.println("-".repeat(100));

//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("ratings:2")).forEach(System.out::println);
//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("ratings:{$eq:2}")).forEach(System.out::println);
//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("ratings:{$eq:2}")).forEach(System.out::println);
//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("ratings:{$gt:2}")).forEach(System.out::println);
//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("ratings:{$gte:2}")).forEach(System.out::println);
//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("ratings:{$lt:2}")).forEach(System.out::println);
//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("ratings:{$lte:2}")).forEach(System.out::println);
//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("ratings:{$in:[4,5]}")).forEach(System.out::println);
//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("ratings:{$all:[1,2]}")).forEach(System.out::println);
//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("ratings:{$size:4}")).forEach(System.out::println);
//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("ratings:{$exists:false}")).forEach(System.out::println);
//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("$or:[{$and:[{ratings:1},{name:{$regex:'n.*'}}]},{$and:[{ratings:2},{name:{$regex:'w.*'}}]}]")).forEach(System.out::println);

				db.commit();
			}

			System.out.println(Document.of("name:malesByName,unique:false,sparse:true,clone:true,filter:[{gender:{$eq:male}}],fields:[{firstName:1},{lastName:1}]"));
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
