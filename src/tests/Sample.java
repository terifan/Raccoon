package tests;

import java.io.File;
import java.util.GregorianCalendar;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.util.Log;


public class Sample
{
	public static void main(String... args)
	{
		try
		{
			Log.LEVEL = 0;

			AccessCredentials accessCredentials = new AccessCredentials("password");

			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.CREATE_NEW, accessCredentials))
			{
				db.save(new _Fruit1K("apple", 52.12));
				db.save(new _Fruit1K("orange", 47.78));
				db.save(new _Fruit1K("banana", 89.45));
				db.save(new _Object1K("test", new GregorianCalendar()));
				db.commit();
			}

			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.OPEN, accessCredentials))
			{
				db.getTables().stream().forEach(e->Log.out.println(e));

				db.list(_Fruit1K.class).stream().forEach(System.out::println);
				db.list(_Object1K.class).stream().forEach(System.out::println);
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}