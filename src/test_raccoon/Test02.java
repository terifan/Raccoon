package test_raccoon;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import org.terifan.logging.ConsoleHandler;
import org.terifan.logging.Level;
import org.terifan.logging.Logger;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonDatabaseProvider;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.blockdevice.RaccoonStorage;
import static test_raccoon._Data.list4346;


public class Test02
{
	public static void main(String... args)
	{
		try
		{
			Logger.getLogger().setHandler(new ConsoleHandler()).setLevel(Level.INFO);

			RaccoonDatabaseProvider provider = new RaccoonDatabaseProvider(new RaccoonStorage().withPassword("pass").inFile("d:\\test.rdb"));

			Random rnd = new Random(1);
			Collections.shuffle(list4346, rnd);

			ArrayList<String> keys = new ArrayList<>();

			try (RaccoonDatabase db = provider.get(DatabaseOpenOption.REPLACE))
			{
				RaccoonCollection people = db.getCollection("people");
				for (int j = 0; j < 1; j++)
				{
					System.out.println(j);
					for (int i = 0; i < 1000; i++)
					{
						String key = list4346.get(i) + "-" + t(rnd, 200);
						keys.add(key);
						people.saveOne(Document.of("_id:$, index:$, text:$", key, i, t(rnd, 500)));
//						if(i==70)people.flush();
//						Thread.sleep(10);
					}
				}
				people.commit();
//				people.printTree();
				System.out.println(people.find().get().size());

				for (int i = 0; i < 1000; i++)
				{
					Document doc = people.findOne(Document.of("_id:$", keys.get(i))).get();
					if (!doc.get("_id").equals(keys.get(i)))
					{
						throw new Exception();
					}
				}

				for (int i = 0; i < 995; i++)
				{
					people.deleteOne(Document.of("_id:$", keys.get(i)));
				}
				people.commit();
//				people.printTree();
				System.out.println(people.find().get().size());

//				RaccoonMap settings = db.getMap("settings");
//				settings.put("count", 5);
//				RaccoonCollection animals = db.getCollection("animals");
//				animals.findOne(Document.of("id:1")).andThen(doc -> System.out.println(doc.get("name")));
//				RaccoonCollection animals = db.getCollection("animals");
//				animals.findMany(Document.of("id:1")).forEach(doc -> System.out.println(doc.get("name")));
//				RaccoonCollection animals = db.getCollection("animals");
//				animals.stream(Document.of("id:1")).forEach(doc -> System.out.println(doc.get("name")));
//				RaccoonCollection animals = db.getCollection("animals");
//				animals.stream().forEach(doc -> System.out.println(doc.get("name")));
			}

			System.out.println("-".repeat(300));

			Logger.getLogger().setLevel(Level.ERROR);
			try (RaccoonDatabase db = provider.withLogging(Level.INFO).get())
			{
				RaccoonCollection people = db.getCollection("people");

				people.printTree();

//				people.find().get().forEach(System.out::println);
				System.out.println(people.find().get().size());

//				System.out.println(db.getMap("settings"));
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}


	private static String t(Random rnd, int aLength)
	{
		char[] alpha = "0123456789abcdefghijklmnopqrstuvwxyzBCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
		char[] buf = new char[aLength + rnd.nextInt(aLength / 10) - aLength / 20];
		for (int i = 0; i < buf.length; i++)
		{
			buf[i] = alpha[rnd.nextInt(alpha.length)];
		}
		return new String(buf);
	}
}
