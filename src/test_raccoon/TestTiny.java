package test_raccoon;

import java.util.concurrent.Future;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonBuilder;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.Result;
import org.terifan.raccoon.document.ObjectId;


public class TestTiny
{
	public static void main(String... args)
	{
		try
		{
			RaccoonBuilder builder = new RaccoonBuilder().pathInTempDir();
			ObjectId id = ObjectId.randomId();

			try (RaccoonDatabase db = builder.get(DatabaseOpenOption.REPLACE))
			{
				RaccoonCollection people = db.getCollection("people");
				people.saveMany(Document.of("_id:[1,1],name:adam"), Document.of("_id:[1,2],name:eve"));
				people.saveOne(Document.of("_id:99999999999,name:steve"));
				people.saveOne(Document.of("_id:admin,name:bob"));
				people.saveOne(Document.of("name:greg").put("_id", id));

				Future<Result> future = people.saveOne(Document.of("name:greg").put("_id", id));
				Result result = future.get();
				System.out.println("******** " + result);
			}

			try (RaccoonDatabase db = builder.get())
			{
				RaccoonCollection people = db.getCollection("people");
				System.out.println(people.findOne(Document.of("_id:[1,1]")).get());
				System.out.println(people.findOne(Document.of("_id:[1,2]")).get());
				System.out.println(people.findOne(Document.of("_id:99999999999")).get());
				System.out.println(people.findOne(new Document("admin")).get());
				System.out.println(people.findOne(new Document(id)).get());

				System.out.println("-".repeat(100));
				people.findMany(Document.of("_id:[1,1]"), new Document("admin"), new Document(99999999999L)).get().forEach(System.out::println);

				System.out.println("-".repeat(100));
				Document admin = new Document("admin");
				if (people.tryFindOne(admin))
				{
					System.out.println("admin=" + admin);
				}

				people.deleteMany(new Document("admin"), new Document(99999999999L));

				System.out.println("-".repeat(100));
				people.forEach(System.out::println);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
