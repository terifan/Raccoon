package test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.terifan.raccoon.BTree;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.document.Array;


public class TestSimple
{
	public static void main(String... args)
	{
		try
		{
			Random rnd = new Random(1);

//			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.REPLACE, null))
//			{
//				for (int i = 0; i < 10_000; i++)
//				{
//					db.getCollection("numbers").save(Document.of("_id:" + Array.of(i/100, i%100, "-".repeat(100))));
//				}
//				db.commit();
//			}

			System.out.println("-".repeat(100));

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.READ_ONLY, null))
			{
//				System.out.println("" + db.getCollectionNames());
//
//				System.out.println(db.getCollection("numbers").size());

				int sz1 = 0;
				for (int i = 0; i < 100; i++)
				{
					List<Document> result = db.getCollection("numbers").find(Document.of("_id:[" + i + "]"));
					sz1 += result.size();
				}

				int sz2 = 0;
				for (int i = 0; i < 10_000; i++)
				{
					List<Document> result = db.getCollection("numbers").find(Document.of("_id:" + Array.of(i/100, i%100)));
					if (result.size()==0)System.out.println(i);
					sz2 += result.size();
				}

				System.out.println("-".repeat(20));
				System.out.println(sz1);
				System.out.println(sz2);

				BTree.RECORD_USE = true;
				System.out.println(db.getCollection("numbers").find(Document.of("_id:[52,70]")).size());
				_Tools.showTree(db.getCollection("numbers")._getImplementation());
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	public static void asasmain(String... args)
	{
		try
		{
			Random rnd = new Random(1);

//			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.REPLACE, null))
//			{
//				db.getCollection("numbers").createIndex(Document.of("name:numberOnly,unique:false,clone:true"), Document.of("number:1"));
////				db.getCollection("numbers").createIndex(Document.of("name:numberSubject,unique:false,clone:true"), Document.of("number:1,subject:1"));
//
//				for (int i = 0; i < 10_000; i++)
//				{
//					int j = rnd.nextInt(1000);
//					String subject = "";
//					for (int k = 0; k < 200; k++)
//					{
//						subject += (char)('a' + rnd.nextInt(25));
//					}
//					db.getCollection("numbers").save(Document.of("number:" + j + ",_id:" + i + ",subject:" + subject + ",body:" + "b".repeat(800)));
//				}
//
////				System.out.println(db.getCollection("numbers").find(Document.of("number:7")).size());
//				db.commit();
//			}
//
//			System.out.println("-".repeat(100));
			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.READ_ONLY, null))
			{
				System.out.println("" + db.getCollectionNames());

				System.out.println(db.getCollection("numbers").find(Document.of("number:7")).size());
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}


	public static void xmain(String... args)
	{
		try
		{
			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.REPLACE, null))
			{
				db.getCollection("people").createIndex(Document.of("name:peopleFirstName,unique:false,clone:true"), Document.of("firstName:1"));
				db.getCollection("people").createIndex(Document.of("name:peopleLastName,unique:false"), Document.of("lastName:1"));
				db.getCollection("people").createIndex(Document.of("name:peopleRating,unique:false,"), Document.of("details/ratings/*:1"));
				db.getCollection("people").createIndex(Document.of("name:peopleLanguage,unique:false"), Document.of("details/language/*:1"));
//				db.createIndex("peopleFirstName", "people", false, "firstName");
//				db.createIndex("peopleLastName", "people", false, "lastName");
//				db.createIndex("peopleRatings", "people", false, "details/ratings/*");
//				db.createIndex("peopleLanguage", "people", false, "details/language/*");

				db.getCollection("people").saveAll(
					Document.of("firstName:adam,lastName:irwing,details:{language:[en,fr],ratings:[1,2]}"),
					Document.of("firstName:eve,lastName:king,details:{language:[en],ratings:[1,3]}"),
					Document.of("firstName:steve,lastName:king,details:{language:[kr],ratings:[1,2,3,4]},_id:7"),
					Document.of("firstName:walter,lastName:black,details:{language:[en],ratings:[3,4]},_id:3219649164198494619"),
					Document.of("firstName:barbara,lastName:black,details:{language:[en,fr,de],ratings:[3]},_id:superuser"),
					Document.of("firstName:bob,lastName:townhill,details:{language:[en,de]}").put("_id", UUID.randomUUID())
				);

				byte[] bytes = Files.readAllBytes(Paths.get("d:\\pictures\\62zqkw9mqo8a1.jpg"));

				db.getCollection("files").save(new Document().put("_id", 1).put("content", bytes));

				db.commit();
			}

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.OPEN, null))
			{
				System.out.println(db.getCollectionNames());

				db.getCollection("people").saveAll(Document.of("firstName:gregor,lastName:king,details:{language:[en,ru],ratings:[1]}"));
				db.getCollection("people").deleteAll(Document.of("_id:7"));
				db.getCollection("people").saveAll(Document.of("_id:superuser,firstName:anne,lastName:black,details:{ratings:[1,2,4]}"));

				System.out.println("-".repeat(100));
				System.out.println("files");
				db.getCollection("files").forEach(System.out::println);
				System.out.println("-".repeat(100));
				System.out.println(db.getCollection("files").get(new Document().put("_id", 1)).getBinary("content").length);
				System.out.println("-".repeat(100));
				System.out.println("people");
				db.getCollection("people").forEach(System.out::println);
				System.out.println("-".repeat(100));
				System.out.println("index:peopleLanguage");
				db.getIndex("peopleLanguage").forEach(System.out::println);
				System.out.println("-".repeat(100));
				System.out.println("index:peopleRatings");
				db.getIndex("peopleRatings").forEach(System.out::println);
				System.out.println("-".repeat(100));
				System.out.println("index:peopleFirstName");
				db.getIndex("peopleFirstName").forEach(System.out::println);
				System.out.println("-".repeat(100));
				System.out.println("index:peopleLastName");
				db.getIndex("peopleLastName").forEach(System.out::println);
				System.out.println("-".repeat(100));
				System.out.println("system:indices");
				db.getCollection("system:indices").forEach(System.out::println);

				System.out.println("-".repeat(100));

				db.getIndex("peopleRatings").find(Document.of("rating:1")).forEach(System.out::println);

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
//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("firstName:{$startsWith:bob, $ignoreCase:true}")).forEach(System.out::println);
//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("firstName:{$endsWith:bob, $ignoreCase:true}")).forEach(System.out::println);
//				System.out.println("-".repeat(100));
//				db.getCollection("people").find(Document.of("firstName:{$contains:bob, $ignoreCase:true}")).forEach(System.out::println);
				db.commit();
			}

//			System.out.println(Document.of("name:malesByName,unique:false,sparse:true,clone:true,filter:[{gender:{$eq:male}}],fields:[{firstName:1},{lastName:1}]"));
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
