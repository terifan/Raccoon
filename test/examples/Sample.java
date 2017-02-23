package examples;

import resources.entities._Object1K;
import resources.entities._Person1K;
import resources.entities._Fruit2K;
import resources.entities._Fruit1K;
import java.io.File;
import java.io.IOException;
import java.util.GregorianCalendar;
import org.terifan.raccoon.Database;
import org.terifan.raccoon.OpenOption;
import org.terifan.raccoon.io.AccessCredentials;
import org.terifan.raccoon.util.Log;
import org.testng.annotations.Test;


public class Sample
{
	@Test(enabled = false)
	public void test() throws IOException
	{
		Log.LEVEL = 0;

		AccessCredentials accessCredentials = new AccessCredentials("password");

		try (Database db = Database.open(new File("c:/temp/sample.db"), OpenOption.CREATE_NEW, accessCredentials))
		{
			db.save(new _Fruit1K("apple", 52.12));
			db.save(new _Fruit1K("orange", 47.78));
			db.save(new _Fruit1K("banana", 89.45));
			db.save(new _Fruit2K("yellow", "lemmon", 89, "bitter"));
			db.save(new _Person1K("Stig", "Helmer", "Stiggatan 10", "41478", "Göteborg", "Sverige", "+46311694797", "+46701649947", "Global Company", 182, 87));
			db.save(new _Object1K("test", new GregorianCalendar()));
			db.commit();
		}

		try (Database db = Database.open(new File("c:/temp/sample.db"), OpenOption.OPEN, accessCredentials))
		{
//			db.list(_Fruit1K.class).stream().forEach(System.out::println);
//			db.list(_Fruit2K.class).stream().forEach(System.out::println);
//			db.list(_Person1K.class).stream().forEach(System.out::println);
//			db.list(_Object1K.class).stream().forEach(System.out::println);
//
//			Log.out.println(db.get(new _Fruit1K("apple")));

			db.list(_Fruit1K.class).forEach(System.out::println);
		}
	}
}