package test_raccoon;

import java.util.concurrent.Future;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonDatabaseProvider;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.blockdevice.RaccoonStorage;
import org.terifan.raccoon.blockdevice.secure.CipherModeFunction;
import org.terifan.raccoon.blockdevice.secure.EncryptionFunction;
import org.terifan.raccoon.document.ObjectId;
import org.terifan.raccoon.result.SaveOneResult;


public class Test03
{
	public static void main(String... args)
	{
		try
		{
			RaccoonDatabaseProvider provider = new RaccoonDatabaseProvider(new RaccoonStorage().withPassword("pass").withCipherMode(CipherModeFunction.ELEPHANT).withEncryption(EncryptionFunction.KUZNECHIK_TWOFISH_AES).withKeyGenerationCost(1024, 32, 1, 1000000).inMemory());
			ObjectId id = ObjectId.randomId();

			try (RaccoonDatabase db = provider.get(DatabaseOpenOption.REPLACE))
			{
				RaccoonCollection people = db.getCollection("people");
				people.saveMany(Document.of("_id:[1,1],name:adam"), Document.of("_id:[1,2],name:eve"));
				people.saveOne(Document.of("_id:99999999999,name:steve"));
				people.saveOne(Document.of("_id:admin,name:bob"));
				people.saveOne(Document.of("name:greg").put("_id", id));

				Future<SaveOneResult> future = people.saveOne(Document.of("name:greg").put("_id", id));
				System.out.println("******** " + future.get());
			}

			try (RaccoonDatabase db = provider.get())
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

				System.out.println("-".repeat(100));

				RaccoonCollection test = db.getCollection("test");
				for (int i = 0; i < 10; i++)
				{
					test.saveOne(Document.of("a:" + i));
				}
				test.iterator().forEachRemaining(d -> System.out.println(d));
				System.out.println(test.find().get().size());
				test.printTree();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
