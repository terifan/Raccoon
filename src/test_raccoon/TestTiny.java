package test_raccoon;

import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonBuilder;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.document.ObjectId;


public class TestTiny
{
	public static void main(String... args)
	{
		try
		{
			RaccoonBuilder builder = new RaccoonBuilder().device("d:\\test.rdb");
			ObjectId id = ObjectId.randomId();

			try (RaccoonDatabase db = builder.get(DatabaseOpenOption.REPLACE))
			{
				RaccoonCollection people = db.getCollection("people");
				people.saveOne(Document.of("_id:[1,1],name:adam"));
				people.saveOne(Document.of("_id:[1,2],name:eve"));
				people.saveOne(Document.of("_id:99999999999,name:steve"));
				people.saveOne(Document.of("_id:admin,name:bob"));
				people.saveOne(Document.of("name:greg").put("_id", id));
			}

			try (RaccoonDatabase db = builder.get())
			{
				RaccoonCollection people = db.getCollection("people");
				System.out.println(people.findOne(Document.of("_id:[1,1]")));
				System.out.println(people.findOne(Document.of("_id:[1,2]")));
				System.out.println(people.findOne(Document.of("_id:99999999999")));
				System.out.println(people.findOne(new Document("admin")));
				System.out.println(people.findOne(new Document(id)));

				System.out.println("-".repeat(100));
				people.findMany(Document.of("_id:[1,1]"), new Document("admin"), new Document(99999999999L)).forEach(System.out::println);

				people.deleteMany(new Document("admin"), new Document(99999999999L));

				System.out.println("-".repeat(100));
				people.find().forEach(System.out::println);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
