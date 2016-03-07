package tests;

import java.io.File;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.util.EntityMap;
import org.terifan.raccoon.util.Log;


public class Sample2
{
	public static void main(String... args)
	{
		try
		{
			Log.LEVEL = 0;

			AccessCredentials accessCredentials = new AccessCredentials("password");

			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.CREATE_NEW, accessCredentials))
			{
//				db.save("test._Fruit1K", new EntityMap()
//					.put("_name", "text")
//					.put("calories", 52.12)
//				);

//				db.save("test._Number1K1D", new EntityMap()
//					.discriminator("odd", true)
//					.key("number", 7)
//					.put("name", "test")
//				);

				db.commit();
			}

			try (Database db = Database.open(new File("d:/sample.db"), OpenOption.OPEN, accessCredentials))
			{
				db.list(_Fruit1K.class).stream().forEach(System.out::println);

//				db.list("test._Fruit1K").stream().forEach(System.out::println);

//				db.list("test._Number1K1D", new EntityMap().discriminator("odd", true)).stream().forEach(System.out::println);
			}
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.out);
		}
	}
}
