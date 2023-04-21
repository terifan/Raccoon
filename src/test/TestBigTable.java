package test;

import java.nio.file.Paths;
import java.util.Random;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;
import org.terifan.raccoon.RaccoonCollection;
import org.terifan.raccoon.document.Document;


//  4:35  commit 1
//  7:17  commit 10
// 11:38 commit 100
public class TestBigTable
{
	public static void main(String... args)
	{
		try
		{
			Random rnd = new Random();
			Runtime r = Runtime.getRuntime();

			int s1 = 100;
			int s2 = 1000_000 / s1;

			long t0 = System.currentTimeMillis();
			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.REPLACE, null))
			{
				RaccoonCollection people = db.getCollection("people");
				RaccoonCollection emails = db.getCollection("emails");
				for (int j = 0; j < s1; j++)
				{
					long t1 = System.currentTimeMillis();
					for (int i = 0; i < s2; i++)
					{
						Document person = _Person.createPerson(rnd);
						Document emailIndex = new Document().put("_id", person.find("personal/contacts/[type=email]/text")).append("person", person.getObjectId("_id"));
						people.save(person);
						emails.save(emailIndex);
					}
					long t2 = System.currentTimeMillis();
					db.commit();
					long t3 = System.currentTimeMillis();
					System.gc();
					System.out.println(s2 * (1 + j) + "\t" + (t2 - t1) + "\t" + (t3 - t2) + "\t" + (t3 - t0) + "\t" + ((r.totalMemory() - r.freeMemory()) / 1024 / 1024));
				}
			}

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.OPEN, null))
			{
//				db.getCollection("people").listAll().forEach(System.out::println);
//				db.getCollection("emails").listAll().forEach(System.out::println);
//				db.getCollection("people").forEach(p -> {});
//				db.getCollection("emails").forEach(p -> {});
				System.out.println(db.getCollection("emails").listAll().size());
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
