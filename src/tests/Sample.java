package tests;

import java.io.File;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.AccessCredentials;


public class Sample
{
	public static void main(String... args)
	{
		try
		{
//			Log.LEVEL = 10;

			AccessCredentials accessCredentials = new AccessCredentials("password");

			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.CREATE_NEW, accessCredentials))
			{
				db.save(new _Fruit1K("apple", 52.12));
				db.save(new _Fruit1K("orange", 47.78));
				db.save(new _Fruit1K("banana", 89.45));
				db.commit();
			}

			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.OPEN, accessCredentials))
			{
				db.list(_Fruit1K.class).stream().forEach(System.out::println);
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}