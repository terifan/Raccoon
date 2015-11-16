package tests;

import java.io.File;
import java.util.List;
import org.terifan.bundle.Bundle;
import org.terifan.bundle.TextDecoder;
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

//				db.bundle("fruits")
//					.key("name", "apple")
//					.putDouble("kcal", 52.12)
//					.save();
//				db.bundle("fruits")
//					.key("name", "orange")
//					.putDouble("kcal", 47.78)
//					.save();
//				db.bundle("fruits", new TextDecoder().unmarshal("{'name':'banana','kcal':89.45}"))
//					.key("name")
//					.save();
//
//				db.commit();
//
//				Bundle b = db.bundle("fruits")
//					.key("name", "orange")
//					.get();
//
//				List<Bundle> result = db.bundle("fruits")
//					.key("name", "orange")
//					.list();
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