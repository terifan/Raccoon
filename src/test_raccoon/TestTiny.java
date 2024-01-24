package test_raccoon;

import java.nio.file.Paths;
import org.terifan.logging.Level;
import org.terifan.logging.Logger;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.RaccoonDatabase;
import org.terifan.raccoon.DatabaseOpenOption;


public class TestTiny
{
	public static void main(String... args)
	{
		try
		{
			Logger.getLogger().setLevel(Level.DEBUG);

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.REPLACE, null))
			{
				db.getCollection("people").save(Document.of("name:adam"));
				db.commit();
			}

			try (RaccoonDatabase db = new RaccoonDatabase(Paths.get("d:\\test.rdb"), DatabaseOpenOption.OPEN, null))
			{
				db.getCollection("people").listAll().forEach(System.out::println);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace(System.out);
		}
	}
}
