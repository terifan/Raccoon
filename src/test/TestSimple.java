package test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.RuntimeDiagnostics;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.document.ObjectId;


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
				db.createIndex("peopleFirstName", "people", false, "firstName");
				db.createIndex("peopleLastName", "people", false, "lastName");

				db.getCollection("people").saveAll(
					Document.of("firstName:adam,lastName:irwing"),
					Document.of("firstName:eve,lastName:king"),
					Document.of("firstName:steve,lastName:king,_id:7"),
					Document.of("firstName:walter,lastName:black,_id:3219649164198494619"),
					Document.of("firstName:barbara,lastName:black,_id:superuser"),
					Document.of("firstName:bob,lastName:townhill").put("_id", UUID.randomUUID())
				);

				byte[] bytes = Files.readAllBytes(Paths.get("d:\\pictures\\4k-daenerys-targaryen.jpg"));

				db.getCollection("files").save(new Document().put("_id", 1).put("content", bytes));

				db.commit();
			}

			System.out.println("-".repeat(100));

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.OPEN, ac))
//			try (RaccoonDatabase db = new RaccoonDatabase(blockDevice, DatabaseOpenOption.OPEN, ac))
			{
				db.getCollection("people").saveAll(
					Document.of("firstName:gregor,lastName:king")
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

				db.getCollection("people").saveAll(
					Document.of("_id:superuser,firstName:anne,lastName:black")
				);

				db.getCollection("people").listAll().forEach(System.out::println);
				db.getCollection("index:peopleFirstName").listAll().forEach(System.out::println);
				db.getCollection("index:peopleLastName").listAll().forEach(System.out::println);

				System.out.println(db.getCollection("files").get(new Document().put("_id", 1)).getBinary("content").length);

				db.commit();
			}

			System.out.println(Document.of("name:malesByName,unique:false,sparse:true,clone:true,filter:[{gender:{$eq:male}}],fields:[{firstName:1},{lastName:1}]"));

//			RuntimeDiagnostics.print();

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
